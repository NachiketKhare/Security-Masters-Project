from datetime import datetime, timedelta
import json
import yfinance as yf
import psycopg2
import pandas as pd
import concurrent.futures
import time
import json
import numpy as np 
from itertools import islice
import psycopg2


# Function to process and store raw data into PostgreSQL
def store_raw_api_response(**kwargs):

    # Fetch data returned by Lambda function
    task_instance = kwargs['ti']
    cloud_function_response = task_instance.xcom_pull(task_ids='invoke_cloud_function')

    # Capture timestamps
    requested_datetime = kwargs['ts']  # Task execution timestamp
    response_datetime = datetime.utcnow()

    # Prepare data for insertion
    api_response_value = json.dumps(cloud_function_response)  # Serialize raw API response as JSON string
    datasource = "FMP"
    api_endpoint = "https://financialmodelingprep.com/api/v3/stock/full/real-time-price?apikey=EnrozQzRrrYmlPKAJkpoJlYFAi2PduM1"  # Replace with your Lambda function name

    # Database connection details
    conn = psycopg2.connect(
        host="dpg-cgfqdvseoogqfc6p8mlg-a.ohio-postgres.render.com",
        port=5432,
        database="financial_data",  # Replace with your database name
        user="Nachiket",
        password="Nachiket'sAccess"
    )

    cursor = conn.cursor()

    # Insert raw API response into raw_api_responses table
    insert_query = """
        INSERT INTO raw_api_responses (
            "RequestedDatetime", "ResponseDatetime", "DataSource", "api_response_values", "api_endpoint"
        ) VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        requested_datetime,
        response_datetime,
        ["FMP"],
        api_response_value,
        api_endpoint
        
    ))

    # Commit transaction and close connection
    conn.commit()
    cursor.close()
    conn.close()


def parse_fmp_stock_data(**kwargs):
    # Fetch data returned by Lambda function
    task_instance = kwargs['ti']
    cloud_function_response = task_instance.xcom_pull(task_ids='invoke_cloud_function')

    response_datetime = datetime.utcnow()
    # api_response_value = json.dumps(lambda_response)  # Serialize raw API response as JSON string
    datasource = "FMP"
    api_endpoint = "https://financialmodelingprep.com/api/v3/stock/full/real-time-price"    

    data = json.loads(cloud_function_response)
    df = pd.DataFrame(data)

    duplicates = df[df['symbol'].duplicated(keep=False)]
    print(f"fmp_df {df}")

    conn = psycopg2.connect(
        host="dpg-cgfqdvseoogqfc6p8mlg-a.ohio-postgres.render.com",
        port=5432,
        database="financial_data",  # Replace with your database name
        user="Nachiket",
        password="Nachiket'sAccess"
    )
    cursor = conn.cursor()

    for index, row in df.iterrows():
        last_updated = datetime.utcfromtimestamp(row['lastUpdated'] / 1000) if 'lastUpdated' in row else None
        # last_sale_time = datetime.utcfromtimestamp(row['lastSaleTime'] / 1000) if 'lastSaleTime' in row else None
        last_sale_time = (datetime.utcfromtimestamp(row['lastSaleTime'] / 1000) if 'lastSaleTime' in row and pd.notna(row['lastSaleTime']) and 0 < row['lastSaleTime'] / 1000 < 32503680000 else None)

        insert_query = """
            INSERT INTO fmp_parsed_stock_data   (
                "DataSource", "ResponseDatetime", "api_endpoint", "symbol", "bidSize", "volume", "bidPrice", "askSize", "askPrice", "lastSalePrice", "lastSaleSize", "lastUpdated", "fmpLast", "lastSaleTime"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("symbol", "DataSource", "ResponseDatetime") DO NOTHING;
        """

        cursor.execute(insert_query, (
            ["FMP"],
            response_datetime,
            api_endpoint,
            row.get('symbol'),
            row.get('bidSize', 0),
            row.get('volume', 0),
            row.get('bidPrice', 0.0),
            row.get('askSize', 0),
            row.get('askPrice', 0.0),     
            row.get('lastSalePrice', 0.0),
            row.get('lastSaleSize', 0),
            last_updated,  # Converted to timestamp
            row.get('fmpLast', 0.0),
            last_sale_time  # Converted to timestamp
 
        ))

    conn.commit()
    cursor.close()
    conn.close()

def parse_yf_stock_data(**kwargs):
    task_instance = kwargs['ti']
    cloud_function_response = task_instance.xcom_pull(task_ids='invoke_cloud_function')

    data = json.loads(cloud_function_response)
    yf_df = pd.DataFrame(data)
    tickers = []
    print(yf_df)

    conn = psycopg2.connect(
        host="dpg-cgfqdvseoogqfc6p8mlg-a.ohio-postgres.render.com",
        port=5432,
        database="financial_data",  # Replace with your database name
        user="Nachiket",
        password="Nachiket'sAccess"
    )
    cursor = conn.cursor()

    # for index, row in yf_df.iterrows():
    #     tickers.append(row['symbol'])

    # def split_list(lst, chunk_size):
    #     return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

    # tickers_5000 = tickers[:200]
    # batch_size = 50
    # ticker_batches = split_list(tickers_5000, batch_size)
    # failed_symbols = []

    def fetch_ticker_data(ticker_batch):
        data = []
        for ticker in ticker_batch:
            try:
                info = yf.Ticker(ticker).info
                extracted_data = {"Ticker": ticker}  # Start with ticker name
                extracted_data.update(info)
                data.append(extracted_data)
            except Exception as e:
                print(f"Error fetching {ticker}: {e}")
                failed_symbols.append(ticker) 
            time.sleep(2)
            
        return data

    # all_data_tickers = []

    # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #     # Submit tasks for each batch
    #     futures = {executor.submit(fetch_ticker_data, batch): batch for batch in ticker_batches}

    #     # Process completed futures
    #     for future in concurrent.futures.as_completed(futures):
    #         try:
    #             result = future.result()
    #             all_data_tickers.extend(result)
    #         except Exception as e:
    #             print(f"Error in thread execution: {e}")
    start_time = time.time()
    end_time = time.time()
    print(f"Data fetching completed in {end_time - start_time:.2f} seconds")
    # print(f"all_data_ticker {all_data_tickers}")

    csv_path = "/opt/airflow/csv_files/yf_500_data.csv"
    data_df_tickers = pd.read_csv(csv_path)
    print(f"all_data_ticker {data_df_tickers}")
    # data_df_tickers = pd.DataFrame(all_data_tickers)
    data_df_tickers['DataSource'] = "Yahoo Finance"
    data_df_tickers['api_endpoint'] = "yfinance.Ticker([ticker])"
    data_df_tickers['ResponseDatetime'] = datetime.utcnow()

    print(f"data_df {data_df_tickers.head()}")

    timestamp_columns = [
        "compensationAsOfEpochDate",
        "exDividendDate",
        "lastFiscalYearEnd",
        "nextFiscalYearEnd",
        "mostRecentQuarter",
        "lastDividendDate",
        "firstTradeDateEpochUtc",
        "governanceEpochDate",
        "sharesShortPreviousMonthDate",
        "lastSplitDate",
        "fundInceptionDate",
        "dateShortInterest"
    ]

    for col in timestamp_columns:
        if col in data_df_tickers.columns:
            data_df_tickers[col] = data_df_tickers[col].apply(
                lambda x: datetime.utcfromtimestamp(x / 1000) if pd.notna(x) and x > 0 else None 
            )

    
    data_df_tickers.drop(columns=["symbol"], inplace=True)
    data_df_tickers.rename(columns={'Ticker':'symbol'}, inplace=True)
    columns = list(data_df_tickers.columns)
    # columns = [col for col in columns if col != "Ticker"]

    placeholders = ", ".join(["%s"] * len(columns))  # Generate %s placeholders for each column
    column_names = ", ".join([f'"{col}"' for col in columns])  # Use double quotes for column names
    
    # Prepare the insert query
    insert_query = f"""
        INSERT INTO "yho_parsed_stock_data" ({column_names})
        VALUES ({placeholders});
    """
    
    # Iterate over DataFrame rows
    for index, row in data_df_tickers.iterrows():
        
        try:
            values = []
            
            for col in columns:
                value = row[col]

                # Convert lists/arrays to JSON
                if isinstance(value, (list, np.ndarray)):
                    value = json.dumps(value)

                # Replace out-of-range integers with None
                elif isinstance(value, (int, float)) and value > 2_147_483_647:
                    print(f"⚠️ Column '{col}' in row {index} has an out-of-range value: {value} (Replaced with None)")
                    value = None  

                # Convert NaN values to None
                elif pd.isna(value):
                    value = None  

                values.append(value)

            # Insert into database
            cursor.execute(insert_query, tuple(values))
        except psycopg2.Error as e:
            print(f"🚨 Database Insertion Error at row {index}: {e}")
            print(f"🔍 Full row data: {row.to_dict()}")
    # Commit the transaction
    conn.commit()
    cursor.close()
    conn.close()