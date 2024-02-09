from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'Tijs_capstone_ingest_dag',
    default_args=default_args,
    description='Daily ingest air quality data from S3 and upload to Snowflake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 2, 9),
    catchup=False,
) as dag:

    # Define the AWS Batch job operator
    run_Tijs_capstone_ingest = BatchOperator(
        task_id='run_Tijs_capstone_ingest',
        job_name='Tijs_capstone_ingest',  # AWS Batch job name
        job_queue='academy-capstone-winter-2024-job-queue',  # AWS Batch job queue
        job_definition='Tijs_capstone_ingest',  # AWS Batch job definition
        region_name='eu-west-1',  # AWS region
        # aws_conn_id='aws_default',
    )

    run_Tijs_capstone_ingest