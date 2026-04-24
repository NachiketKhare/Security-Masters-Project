from airflow import DAG
from airflow.operators.python import PythonOperator
from py_files.data_quality_check import fetch_datetimes
from py_files.data_quality_check import compare_all_consecutive_responses
from py_files.data_quality_check import extract_data
from py_files.data_quality_check import compare_responses
from datetime import datetime, timedelta
import psycopg2
import pandas as pd


with DAG(
    'compare_api_responses',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Compare two API responses from parsed_stock_data',
    schedule_interval=None,  
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['comparison', 'api'],
) as dag:
    
    fetch_all_consecutive_datetimes_task = PythonOperator(
        task_id='fetch_datetime',
        python_callable=fetch_datetimes,
        op_kwargs={'mode': 'consecutive'},
    )

    compare_consecutive_responses_task = PythonOperator(
        task_id='compare_all_consecutive',
        python_callable=compare_all_consecutive_responses,
    )

    fetch_same_interval_datetimes_task = PythonOperator(
        task_id='fetch_datetimes',
        python_callable=fetch_datetimes,
        op_kwargs={'mode': 'same_interval'}, 
    )

    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    compare_responses_task = PythonOperator(
        task_id='compare_responses',
        python_callable=compare_responses,
    )

    # fetch_all_consecutive_datetimes_task >> compare_consecutive_responses_task
    fetch_same_interval_datetimes_task >> extract_data_task >> compare_responses_task

