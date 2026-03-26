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


    cleanup_data = BashOperator(
        task_id="cleanup_data",
        bash_command=(
            "rm -rf /opt/airflow/data/bronze/* "
            "/opt/airflow/data/silver/* "
            "/opt/airflow/data/gold/* "
            "/opt/airflow/data/checkpoints/bronze/* && "
            "mkdir -p /opt/airflow/data/bronze "
            "/opt/airflow/data/silver "
            "/opt/airflow/data/gold "
            "/opt/airflow/data/checkpoints/bronze"
        ),
    )

    produce_events = BashOperator(
        task_id="produce_events",
        bash_command=(
        "cd /app && "
        "export KAFKA_BOOTSTRAP_SERVERS=kafka:29092 && "
        "python3 src/producer/faker_producer.py "
        "--num-events 500 "
        "--sleep-seconds 0.1"
        ),
    )

    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
        "cd /app && "
        "export KAFKA_BOOTSTRAP_SERVERS=kafka:29092 && "
        "spark-submit "
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
        "src/streaming/bronze_ingest.py "
        "--run-seconds 90"
        ),
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=(
            "cd /app && "
            "spark-submit src/streaming/silver_transform.py"
        ),
    )

    gold_aggregate = BashOperator(
        task_id="gold_aggregate",
        bash_command=(
            "cd /app && "
            "spark-submit src/streaming/gold_aggregate.py"
        ),
    )

    cleanup_data >> produce_events >> bronze_ingest >> silver_transform >> gold_aggregate