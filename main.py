
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable 
import requests
import pandas as pd
import psycopg2
import boto3



def extract_job_data():
    url = "https://jsearch.p.rapidapi.com/search"

    querystring = {"query":"Data Engineer or Data Analyst job in Canada","page":"1","num_pages":"1","date_posted":"today","job_titles":"data engineer,data analyst"}

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
def load_to_data_warehouse():
    #configure variables 
    s3_bucket = Variable.get("s3_bucket_transformed_data")
    s3_key = 'data_csv.csv'

    redshift_endpoint = '************'
    redshift_port = 5439
    redshift_db = 'dev'
    redshift_user = 'admin'
    redshift_password = Variable.get('redshift_password')
    redshift_table = 'job_data'

    # Initialize AWS clients
    #s3_hook = S3Hook(aws_conn_id='aws_s3')
    s3_client = boto3.client('s3')

    # Download the CSV file from S3
    local_file_path = Variable.get('local_file_path')  # Path to store the file locally
    s3_client.download_file(s3_bucket, s3_key, local_file_path)

    # establish a connection to redshift database
    redshift_conn = psycopg2.connect(
        host=redshift_endpoint,
        port=redshift_port,
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password
    )

    # create a cursor
    redshift_cursor = redshift_conn.cursor()

    # Load the CSV file into Redshift
    copy_query = f"COPY {redshift_table} FROM '{local_file_path}' CSV DELIMITER ',' IGNOREHEADER 1;"
    redshift_cursor.execute(copy_query)

    # close connection
    redshift_conn.commit()
    redshift_cursor.close()
    redshift_conn.close()

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