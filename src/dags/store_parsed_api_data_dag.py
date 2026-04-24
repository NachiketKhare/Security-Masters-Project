from airflow import DAG
# from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.google.cloud.operators.functions import CloudFunctionsInvokeFunctionOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
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
from py_files.trust_ranking import calculate_trust_ranking
from py_files.aDD_market_prices_concept_values import process_market_data
from py_files.store_api_data import store_raw_api_response
from py_files.store_api_data import parse_fmp_stock_data
from py_files.store_api_data import parse_yf_stock_data
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}



# Define the DAG
with DAG(
    'call_cloud_function_store_postgres',
    default_args=default_args,
    description='Trigger Cloud Function and store raw API response in PostgreSQL every 2 hours',
    schedule_interval='0 */2 * * *',  # Every 2 hours
    start_date=datetime(2025, 1, 15),
    catchup=False,
) as dag:

    # Task 1: Invoke Cloud Function
    invoke_cloud_function = HttpOperator(
        task_id="invoke_cloud_function",
        method="GET",
        http_conn_id="google_cloud_function",  
        endpoint="/fetch_fmp_data",
        do_xcom_push=True
    )

    # Task 2: Store raw API response into PostgreSQL
    store_raw_data = PythonOperator(
        task_id='store_raw_api_response',
        python_callable=store_raw_api_response,
        provide_context=True
    )

    # Add task for parsing and storing data
    parse_fmp_store_data_task = PythonOperator(
        task_id='parse_fmp_store_stock_data',
        python_callable=parse_fmp_stock_data,
        provide_context=True
    )

    parse_yf_stock_data_task = PythonOperator(
        task_id='parse_yf_store_stock_data',
        python_callable=parse_yf_stock_data,
        provide_context=True
    )

    calculate_trust_ranking_task = PythonOperator(
        task_id='calculate_trust_ranking',
        python_callable=calculate_trust_ranking,
        provide_context=True
    )

    aDD_market_prices_concept_values_task = PythonOperator(
        task_id='aDD_market_prices_concept_values_task',
        python_callable=process_market_data,
        provide_context=True
    )

    
    # Define task dependencies
    # invoke_lambda >> parse_fmp_store_data_task >> parse_yf_stock_data_task
    invoke_cloud_function >> parse_fmp_store_data_task

