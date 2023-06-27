from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable 
import requests
import pandas as pd

with DAG('extract_dag', start_date=datetime(2023, 6, 17), schedule_interval='@daily') as dag:
    task1 = PythonOperator(
        task_id='extract_job_data',
        python_callable=extract_job_data
    )
    task2 = PythonOperator(
        task_id='load_json_to_s3',
        python_callable=upload_json_to_s3
        
    )
    task3 = PythonOperator(
        task_id='load_csv_to_s3',
        python_callable=upload_csv_to_s3
        
    )
     task4 = PythonOperator(
        task_id='load_file_task',
        python_callable=load_to_data_warehouse,

    )

task1 >> task2 >> task3 >> task4
