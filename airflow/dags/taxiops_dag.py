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
    tags=["taxiops", "kafka", "spark", "airflow"],
) as dag:

    produce_events = BashOperator(
        task_id="produce_events",
        bash_command=(
            "cd /app && "
            "python3 src/producer/faker_producer.py "
            "--num-events 500 "
            "--sleep-seconds 0.1"
        ),
    )

    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
            "cd /app && "
            "python3 src/streaming/bronze_ingest.py "
            "--run-seconds 90"
        ),
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command="cd /app && python3 src/streaming/silver_transform.py",
    )

    gold_aggregate = BashOperator(
        task_id="gold_aggregate",
        bash_command="cd /app && python3 src/streaming/gold_aggregate.py",
    )

    cleanup_data = BashOperator(
    task_id="cleanup_data",
    bash_command=(
        "cd /app && "
        "rm -rf data/bronze data/silver data/gold data/checkpoints"
    ),
)

    cleanup_data >> bronze_ingest >> produce_events >> silver_transform >> gold_aggregate