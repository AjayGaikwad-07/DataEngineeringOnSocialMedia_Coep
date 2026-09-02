"""
airflow/dags/social_media_pipeline_dag.py

Schedules the full SocialMediaPipeline flow:
  collect -> kafka producer -> kafka consumer -> spark process/load -> dashboard

Assumes the project folder is mounted into the Airflow containers at
/opt/airflow/project (see the docker-compose.yml volume mapping), and
that producer/consumer/dashboard scripts sit where the PDF's folder
structure puts them:
  producer/producer.py
  consumer/consumer.py
  spark/process_and_load.py
  dashboard/dashboard.py

If cursor named these scripts differently, just adjust the paths below
— the DAG shape (task order + retries) stays the same.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"

default_args = {
    "owner": "ajayg",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="social_media_pipeline",
    description="Collect -> Kafka -> Spark -> Postgres -> Dashboard",
    default_args=default_args,
    schedule_interval="@daily",   # change to e.g. "0 */6 * * *" for every 6 hours
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["de-assignment", "social-media"],
) as dag:

    collect_data = BashOperator(
        task_id="collect_data",
        bash_command=f"python {PROJECT_DIR}/data/collect.py",
    )

    produce_to_kafka = BashOperator(
        task_id="produce_to_kafka",
        bash_command=f"python {PROJECT_DIR}/producer/producer.py",
    )

    consume_from_kafka = BashOperator(
        task_id="consume_from_kafka",
        bash_command=f"python {PROJECT_DIR}/consumer/consumer.py",
    )

    spark_process = BashOperator(
        task_id="spark_process_and_load",
        bash_command=(
            "spark-submit --packages org.postgresql:postgresql:42.7.3 "
            f"{PROJECT_DIR}/spark/process_and_load.py"
        ),
    )

    generate_dashboard = BashOperator(
        task_id="generate_dashboard",
        bash_command=f"python {PROJECT_DIR}/dashboard/dashboard.py",
    )

    collect_data >> produce_to_kafka >> consume_from_kafka >> spark_process >> generate_dashboard
