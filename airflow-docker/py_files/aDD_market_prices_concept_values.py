import pandas as pd
import psycopg2
from datetime import datetime, timezone, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2.extras import execute_values

def get_db_connection():
    return psycopg2.connect(
        host="dpg-cgfqdvseoogqfc6p8mlg-a.ohio-postgres.render.com",
        port=5432,
        database="financial_data",
        user="Nachiket",
        password="Nachiket'sAccess"
    )

# def fetch_best_ranked_datasource():
   
#     conn = get_db_connection()
#     query = """
#         SELECT "aDD_concept", "DataSource"
#         FROM "aDD_concept_datasource_trust_ranking"
#         WHERE "ranking_end_datetime" IS NULL
#         ORDER BY "ranking";  -- Lowest ranking is most trusted
#     """
#     df = pd.read_sql(query, conn)
#     conn.close()
#     return df

def fetch_mapping():
    conn = get_db_connection()
    query = """
        SELECT "DataSource", "data_field", "aDD_concept"
        FROM "datasource_field_to_aDD_concept_map"
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # df["DataSource"] = df["DataSource"].apply(lambda x: x.strip("{}").split(","))  # Convert from array format
    df = df.explode("DataSource")  # Normalize multiple data sources

    df["DataSource"] = df["DataSource"].astype(str).str.strip()

    return df

def fetch_latest_parsed_data(data_source):
    conn = get_db_connection()

    if data_source == "FMP":
        query = """
            SELECT *
            FROM "fmp_parsed_stock_data"
            WHERE "ResponseDatetime" = (SELECT MAX("ResponseDatetime") FROM "fmp_parsed_stock_data");
        """

    elif data_source == "Yahoo Finance":
        query = """
            WITH latest_data AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY "ResponseDatetime" DESC) AS row_num
                FROM "yho_parsed_stock_data"
            )
            SELECT * FROM latest_data WHERE row_num = 1;
        """
    else:
        raise ValueError(f"Unknown DataSource: {data_source}")
    
    df = pd.read_sql(query, conn) 
    
    # df["datasource_endpoint"] = "https://financialmodelingprep.com/api/v3/stock/full/real-time-price"
    # df.drop(columns=['DataSource', 'row_num', 'ResponseDatetime', 'api_endpoint', 'lastUpdated', 'fmpLast', 'lastSaleTime', 'lastSaleSize'])
    conn.close()
    return df

# def fetch_latest_yf_data_from_csv():
#     csv_path = "/opt/airflow/csv_files/yf_data_2000_1pm.csv"
#     df = pd.read_csv(csv_path)
#     df["datasource_endpoint"] = "https://query1.finance.yahoo.com/v7/finance/quote"

#     return df

def insert_data_to_postgres(final_df):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Define the INSERT query
    insert_query = """
    INSERT INTO aDD_market_prices_concept_values (
        "DataSource", "symbol", "asof_datetime", "last_trade_price", 
        "current_nbbo_ask_p", "current_nbbo_ask_s", 
        "current_nbbo_bid_p", "current_nbbo_bid_si", "last_trade_volume"
    ) VALUES %s
    ON CONFLICT ("symbol", "asof_datetime") 
    DO UPDATE SET 
        "last_trade_price" = EXCLUDED."last_trade_price",
        "current_nbbo_ask_p" = EXCLUDED."current_nbbo_ask_p",
        "current_nbbo_ask_s" = EXCLUDED."current_nbbo_ask_s",
        "current_nbbo_bid_p" = EXCLUDED."current_nbbo_bid_p",
        "current_nbbo_bid_si" = EXCLUDED."current_nbbo_bid_si",
        "last_trade_volume" = EXCLUDED."last_trade_volume";
    """

    # Convert DataFrame to a list of tuples
    data_tuples = [tuple(row) for row in final_df.itertuples(index=False, name=None)]

    # Execute batch insert
    execute_values(cursor, insert_query, data_tuples)

    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()

def process_market_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    mapping_df = fetch_mapping()

    fmp_df = fetch_latest_parsed_data("FMP")
    yf_df = fetch_latest_parsed_data("Yahoo Finance")

    print(f"mapping_df {mapping_df}")
    print(f"fmp df {fmp_df}")

    relevant_fmp_fields = set(mapping_df[mapping_df["DataSource"] == "FMP"]["data_field"].unique())
    fmp_selected = fmp_df[["symbol", "ResponseDatetime"] + list(relevant_fmp_fields)]
    fmp_melted = fmp_selected.melt(id_vars=["symbol", "ResponseDatetime"], 
                                   value_vars=relevant_fmp_fields, 
                                   var_name="data_field", 
                                   value_name="value")
    
    fmp_merged = fmp_melted.merge(mapping_df[mapping_df["DataSource"] == "FMP"], 
                                  on="data_field", how="inner")

    # 2️⃣ **Map Yahoo Finance Data to aDD_concepts**
    relevant_yf_fields = set(mapping_df[mapping_df["DataSource"] == "Yahoo Finance"]["data_field"].unique())
    yf_selected = yf_df[["symbol", "ResponseDatetime"] + list(relevant_yf_fields)]
    yf_melted = yf_selected.melt(id_vars=["symbol", "ResponseDatetime"], 
                                 value_vars=relevant_yf_fields, 
                                 var_name="data_field", 
                                 value_name="value")
    yf_merged = yf_melted.merge(mapping_df[mapping_df["DataSource"] == "Yahoo Finance"], 
                                on="data_field", how="inner")

    print(f"fmp merged {fmp_merged.columns}")
    print(f"yfmerged {yf_merged.columns}")

    # ✅ **At this point, `fmp_merged` and `yf_merged` have `symbol`, `ResponseDatetime`, `aDD_concept`, and `value`.**
     
    # 3️⃣ **Merge both DataFrames & Prioritize FMP Values**
    combined_df = fmp_merged.merge(yf_merged, on=["symbol", "aDD_concept"], suffixes=("_FMP", "_YF"), how="outer")

    print(combined_df.columns)

    for concept in ["last_trade_price", "current_nbbo_bid_price", "current_nbbo_ask_price",
                    "current_nbbo_bid_size", "current_nbbo_ask_size", "last_trade_volume"]:
        # Create a mask to filter only rows where `aDD_concept` matches `concept`
        mask = combined_df["aDD_concept"] == concept
        # Use .loc to assign values only to the matching rows
        combined_df.loc[mask, concept] = combined_df.loc[mask, "value_FMP"].combine_first(combined_df.loc[mask, "value_YF"])

    print(f"combined_df after masking {combined_df}")
    # ✅ Now `combined_df` has symbols mapped to aDD_concepts with values from the best available source.

    final_df = combined_df.groupby("symbol").first().reset_index()

    final_df = final_df.drop(columns=['data_field_FMP', 'aDD_concept', 'ResponseDatetime_YF', 'data_field_YF', 'DataSource_YF','value_YF', 'value_FMP'])
    final_df = final_df.rename(columns={'ResponseDatetime_FMP':'asof_datetime','DataSource_FMP':'DataSource'})
    # final_df.to_csv("/opt/airflow/csv_files/combined_df.csv")


