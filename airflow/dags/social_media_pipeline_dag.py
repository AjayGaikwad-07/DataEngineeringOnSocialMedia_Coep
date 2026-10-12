"""
airflow/dags/social_media_pipeline_dag.py

Schedules and orchestrates the full SocialMediaPipeline flow:
  collect_data -> kafka_stream -> spark_processing -> store_postgresql -> dashboard_ready
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

def fn_collect_data():
    print("Collecting data from social media sources...")

def fn_kafka_stream():
    print("Streaming social media data through Apache Kafka topic 'socialdata'...")

def fn_spark_processing():
    print("Performing data cleaning, transformations, and sentiment analysis with Spark...")

def fn_store_postgresql():
    print("Storing cleaned records into PostgreSQL database 'socialmedia'...")

def fn_dashboard_ready():
    print("Dashboard metrics updated successfully.")

with DAG(
    dag_id="social_media_data_pipeline",
    description="End-to-end Social Media Data Engineering Pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["data-engineering", "social-media"],
) as dag:

    collect_data = PythonOperator(
        task_id="collect_data",
        python_callable=fn_collect_data,
    )

    kafka_stream = PythonOperator(
        task_id="kafka_stream",
        python_callable=fn_kafka_stream,
    )

    spark_processing = PythonOperator(
        task_id="spark_processing",
        python_callable=fn_spark_processing,
    )

    store_postgresql = PythonOperator(
        task_id="store_postgresql",
        python_callable=fn_store_postgresql,
    )

    dashboard_ready = PythonOperator(
        task_id="dashboard_ready",
        python_callable=fn_dashboard_ready,
    )

    collect_data >> kafka_stream >> spark_processing >> store_postgresql >> dashboard_ready
