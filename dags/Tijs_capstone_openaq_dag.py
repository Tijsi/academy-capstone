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
    'Tijs_capstone_ingest_openaq_dag',
    default_args=default_args,
    description='Daily ingest air quality data from S3 and upload to Snowflake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 2, 9),
    catchup=False,
) as dag:

        # Define the AWS Batch job operator
    run_Tijs_capstone_openaq_api_to_s3 = BatchOperator(
        task_id='Tijs_capstone_ingest_openaq',
        job_name='Tijs_capstone_ingest_openaq',  # AWS Batch job name
        job_queue='academy-capstone-winter-2024-job-queue',  # AWS Batch job queue
        job_definition='Tijs_capstone_ingest_openaq',  # AWS Batch job definition
        overrides={
            'environment': [  # Set your environment variables here
                {
                    'name': 'DATE_FROM',
                    'value': '{{ execution_date.subtract(days=1).strftime("%Y-%m-%d") }}',
                },
                {
                    'name': 'DATE_TO',
                    'value': '{{ execution_date.strftime("%Y-%m-%d") }}',
                },
            ],
        },
        region_name='eu-west-1',  # AWS region
    )

    run_Tijs_capstone_openaq_api_to_s3