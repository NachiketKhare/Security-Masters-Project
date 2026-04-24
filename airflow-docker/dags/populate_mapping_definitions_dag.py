from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import json
import psycopg2
from py_files.mapping_and_definations import insert_or_update_mappings
from py_files.mapping_and_definations import insert_update_definitions


default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'populate_mapping_definitions_nbbo',
    default_args=default_args,
    description='DAG to populate field mapping definitions and NBBO values data',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['mapping', 'NBBO'],
) as dag:
    
    update_mapping_task = PythonOperator(
        task_id = 'insert_or_update_mappings',
        python_callable=insert_or_update_mappings
    )

    update_definitions_task = PythonOperator(
        task_id='insert_or_update_definitions',
        python_callable=insert_update_definitions
    )
    
    update_mapping_task >> update_definitions_task
    