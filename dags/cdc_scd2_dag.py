"""
SCD Type 2 DAG using Spark
Runs Spark job to process CDC data into Iceberg SCD2 table (insert, update, delete)
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import os

logger = logging.getLogger(__name__)

# Default args
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# # Paths
SPARK_SCRIPT = '/opt/airflow/libs/scd2_spark_processor.py'
PYTHON_ENV = '/opt/airflow_venv/bin/python3'

os.environ['PYSPARK_PYTHON'] = '/opt/airflow_venv/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/airflow_venv/bin/python3.9'


def check_staging_data():
    """Check if staging data exists before running Spark"""
    import boto3

    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9002',
        aws_access_key_id='admin',
        aws_secret_access_key='your_minio_secret_here'
    )

    response = s3_client.list_objects_v2(
        Bucket='streaming',
        Prefix='staging/sales_cdc/',
        MaxKeys=1
    )

    has_data = 'Contents' in response and len(response['Contents']) > 0

    if has_data:
        logger.info("✅ Staging data found, proceeding with Spark job")
    else:
        logger.warning("⚠️ No staging data found, skipping Spark job")

    return has_data


# Define DAG
with DAG(
    dag_id='scd2_sales_spark_processing',
    default_args=default_args,
    description='SCD Type 2 processing using Spark and Iceberg',
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags={'scd2', 'sales', 'spark', 'iceberg'},
) as dag:

    # Task 1: Check if staging has data
    check_data_task = PythonOperator(
        task_id='check_staging_data',
        python_callable=check_staging_data,
    )

    # Task 2: Run Spark SCD2 processor
    spark_job_task = BashOperator(
        task_id='run_spark_scd2',
        bash_command=f'{PYTHON_ENV} {SPARK_SCRIPT}',
    )

    check_data_task >> spark_job_task
