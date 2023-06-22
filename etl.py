from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable 
import requests
import pandas as pd




def extract_job_data():
    url = "https://jsearch.p.rapidapi.com/search"

    querystring = {"query":"Data Engineer or Data Analyst job in USA or Canada","page":"1","num_pages":"1","date_posted":"today","job_titles":"data engineer,data analyst"}

    headers = {
        "X-RapidAPI-Key": "38ff82da04mshabfe208703e2ebdp1220e0jsnb2a20a82a858",
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    raw_data = response.json()
    data = raw_data['data']
    df = pd.DataFrame(data)
    path = Variable.get('raw_data_json')
    df.to_json(path, orient='records')

def upload_json_to_s3(): 
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    bucket_name = Variable.get("s3_bucket_raw_data")
    s3_hook.load_file(
        filename=Variable.get('raw_data_json'),
        key='data_json.json',
        bucket_name=bucket_name,
        replace=True
    )

def upload_csv_to_s3(): 
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    bucket_name = Variable.get("s3_bucket_transformed_data")
    path = Variable.get('raw_data_json')
    fileName = Variable.get('transformed_data')
    df = pd.read_json(path)
    col_of_interest = ['employer_website', 'job_id', 'job_employment_type', 'job_title',
                          'job_apply_link', 'job_description', 'job_city', 'job_country',
                          'job_posted_at_timestamp', 'employer_company_type']
    df = df[col_of_interest]
    df.to_csv(fileName, index=False)
    s3_hook.load_file(
        filename= fileName,
        key='data_csv.csv',
        bucket_name=bucket_name,
        replace=True
    )


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

task1 >> task2 >> task3