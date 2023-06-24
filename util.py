import psycopg2

redshift_endpoint = 'kwasako-endpoint-7xofvfnpr8ozsghmvehc.265037400333.eu-north-1.redshift-serverless.amazonaws.com'
redshift_port = 5439
redshift_db = 'dev'
redshift_user = 'admin'
redshift_password = Variable.get('redshift_password')
redshift_table = 'job_data'

redshift_conn = psycopg2.connect(
        host=redshift_endpoint,
        port=redshift_port,
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password
    )
