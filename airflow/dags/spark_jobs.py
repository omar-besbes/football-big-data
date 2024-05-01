from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG('spark_dag', default_args=default_args, schedule_interval='@daily') as dag:
    spark_job = SparkSubmitOperator(
        application='/opt/bitnami/spark/jobs/spark_job.py', # Path in the Spark container
        name='process_events',
        conn_id='spark_default',
        task_id='spark_job_task',
        execution_timeout=600
    )

spark_job
