from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="taxiops_pipeline",
    default_args=default_args,
    description="TaxiOps Kafka -> Bronze -> Silver -> Gold pipeline",
    start_date=datetime(2025, 3, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["taxiops", "kafka", "spark", "airflow"],
) as dag:

    cleanup_data = BashOperator(
        task_id="cleanup_data",
        bash_command=(
            "rm -rf "
            "/opt/pipeline/data/bronze "
            "/opt/pipeline/data/silver "
            "/opt/pipeline/data/gold "
            "/opt/pipeline/data/checkpoints"
        ),
    )

    produce_events = BashOperator(
        task_id="produce_events",
        bash_command=(
            "python3 /opt/pipeline/src/producer/faker_producer.py "
            "--num-events 500 "
            "--sleep-seconds 0.1"
        ),
    )

    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
            "python3 /opt/pipeline/src/streaming/bronze_ingest.py "
            "--run-seconds 90"
        ),
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command="python3 /opt/pipeline/src/streaming/silver_transform.py",
    )

    gold_aggregate = BashOperator(
        task_id="gold_aggregate",
        bash_command="python3 /opt/pipeline/src/streaming/gold_aggregate.py",
    )

    cleanup_data >> produce_events >> bronze_ingest >> silver_transform >> gold_aggregate