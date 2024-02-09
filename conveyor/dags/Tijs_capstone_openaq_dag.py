from airflow import DAG
from conveyor.operators import ConveyorContainerOperatorV2
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

    run_Tijs_capstone_ingest = ConveyorContainerOperatorV2(
        task_id="ingest_api_data",
        arguments=["--date", "{{ ds }}"],
        aws_role="capstone_conveyor",
        instance_type='mx.micro',
        env_vars=
                {
                    'DATE_FROM': '{{ execution_date.subtract(days=1).strftime("%Y-%m-%d") }}',
                    'DATE_TO': '{{ execution_date.strftime("%Y-%m-%d") }}',
                }
    )

    run_Tijs_capstone_ingest