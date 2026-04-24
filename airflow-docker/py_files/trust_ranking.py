import pandas as pd
import psycopg2
from datetime import datetime, timezone, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator

#Database Connection
def get_db_connection():
    return psycopg2.connect(
        host="dpg-cgfqdvseoogqfc6p8mlg-a.ohio-postgres.render.com",
        port=5432,
        database="financial_data",
        user="Nachiket",
        password="Nachiket'sAccess"
    )

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

def fetch_latest_fmp_data():
    conn = get_db_connection()
    query = """
        WITH latest_data AS (
            SELECT "symbol", "bidPrice", "bidSize", "askPrice", "askSize", "lastSaleSize", ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY "ResponseDatetime" DESC) AS row_num
            FROM "fmp_parsed_stock_data"
        )
        SELECT * FROM latest_data WHERE row_num = 1;
    """

    df = pd.read_sql(query, conn)
    df["datasource_endpoint"] = "https://financialmodelingprep.com/api/v3/stock/full/real-time-price"
    # df.drop(columns=['DataSource', 'row_num', 'ResponseDatetime', 'api_endpoint', 'lastUpdated', 'fmpLast', 'lastSaleTime', 'lastSaleSize'])
    conn.close()
    return df

def fetch_latest_yf_data_from_csv():
    csv_path = "/opt/airflow/csv_files/yf_data_2000_1pm.csv"
    df = pd.read_csv(csv_path)
    df["datasource_endpoint"] = "https://query1.finance.yahoo.com/v7/finance/quote"

    return df

def calculate_trust_ranking():

    conn = get_db_connection()
    cursor = conn.cursor()

    mapping_df = fetch_mapping()
    fmp_df = fetch_latest_fmp_data()
    yf_df = fetch_latest_yf_data_from_csv()
    
    # print(f"fmp df: {fmp_df.to_string()}")

    relevant_fmp_data_fields = set(mapping_df[mapping_df["DataSource"] == "FMP"]["data_field"].unique())

    fmp_selected = fmp_df[["symbol", "datasource_endpoint"] + list(relevant_fmp_data_fields)] 

    fmp_melted = fmp_selected.melt(id_vars=["symbol", "datasource_endpoint"], 
                                   value_vars=relevant_fmp_data_fields, 
                                   var_name="data_field", 
                                   value_name="value")

    fmp_merged = fmp_melted.merge(mapping_df[mapping_df["DataSource"] == "FMP"], 
                                  on="data_field", how="inner")

    relevant_yf_fields = set(mapping_df[mapping_df["DataSource"] == "Yahoo Finance"]["data_field"].unique())

    yf_selected = yf_df[["symbol", "datasource_endpoint"] + list(relevant_yf_fields)]

    yf_melted = yf_selected.melt(id_vars=["symbol", "datasource_endpoint"], 
                                 value_vars=relevant_yf_fields, 
                                 var_name="data_field", 
                                 value_name="value")

    yf_merged = yf_melted.merge(mapping_df[mapping_df["DataSource"] == "Yahoo Finance"], 
                            on="data_field", how="inner")
    
    combined_df = pd.concat([fmp_merged, yf_merged])
    combined_df["ranking"] = combined_df.groupby(["symbol", "aDD_concept"])["value"].rank(method="first", ascending=False)

    overall_ranking = (
            combined_df.groupby(["aDD_concept", "DataSource", "datasource_endpoint"])["ranking"]
            .mean()  # Take average ranking per aDD_concept for each DataSource
            .reset_index()
        )

    overall_ranking["overall_ranking"] = (
            overall_ranking.groupby("aDD_concept")["ranking"]
            .rank(method="dense", ascending=True)
        )

    overall_ranking["ranking_start_datetime"] = datetime.now(timezone.utc)
    overall_ranking["ranking_end_datetime"] = None

    print(fmp_merged)
    fmp_merged.to_csv("/opt/airflow/csv_files/fmp_merged.csv")
    yf_merged.to_csv("/opt/airflow/csv_files/yf_merged.csv")

    for index, row in overall_ranking.iterrows():
        aDD_concept = row["aDD_concept"]
        DataSource = row["DataSource"]
        new_ranking = row["overall_ranking"]
        datasource_endpoint = row["datasource_endpoint"]
        ranking_start_datetime = row["ranking_start_datetime"]


        cursor.execute("""
            SELECT "ranking" FROM "aDD_concept_datasource_trust_ranking"
            WHERE "aDD_concept" = %s AND "DataSource" = %s 
            AND "ranking_end_datetime" IS NULL;
        """, (aDD_concept, DataSource))

        existing_rank = cursor.fetchone()

        if existing_rank:
            existing_rank = existing_rank[0]

            if existing_rank != new_ranking:
                cursor.execute("""
                    UPDATE "aDD_concept_datasource_trust_ranking"
                    SET "ranking_end_datetime" = %s
                    WHERE "aDD_concept" = %s AND "DataSource" = %s 
                    AND "ranking_end_datetime" IS NULL;
                """, (datetime.now(timezone.utc), aDD_concept, DataSource))

        cursor.execute("""
            INSERT INTO "aDD_concept_datasource_trust_ranking"
            ("aDD_concept", "DataSource", "datasource_endpoint", "ranking", "ranking_start_datetime", "ranking_end_datetime")
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (aDD_concept, DataSource, datasource_endpoint, new_ranking, ranking_start_datetime, None))
        
    

    conn.commit()
    cursor.close()
    conn.close()
    

