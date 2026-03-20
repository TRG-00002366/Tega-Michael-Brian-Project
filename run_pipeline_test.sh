#!/bin/bash

echo "=============================="
echo "STARTING PIPELINE TEST"
echo "=============================="

# Step 1: Start Docker services
echo "Starting Docker services..."
docker compose up -d

sleep 10

# Step 2: Check Kafka
echo "Checking Kafka..."
docker compose ps | grep kafka

# Step 3: Produce data
echo "Producing data to Kafka..."
docker compose exec -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 airflow-scheduler \
python /app/src/producer/faker_producer.py --num-events 100 --sleep-seconds 0.2

echo "Data production complete"

# Step 4: Run Bronze
echo " Running Bronze Layer..."
docker compose exec airflow-scheduler spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
/app/src/streaming/bronze_ingest.py --run-seconds 60

echo "Bronze complete"

# Step 5: Check Bronze output
echo "Checking Bronze output..."
docker compose exec airflow-scheduler find /opt/airflow/data/bronze -type f

# Step 6: Run Silver
echo "Running Silver Layer..."
docker compose exec airflow-scheduler spark-submit \
/app/src/streaming/silver_transform.py

echo "Silver complete"

# Step 7: Check Silver output
echo "Checking Silver output..."
docker compose exec airflow-scheduler find /opt/airflow/data/silver -type f

# Step 8: Run Gold
echo "Running Gold Layer..."
docker compose exec airflow-scheduler spark-submit \
/app/src/streaming/gold_aggregate.py

echo "Gold complete"

# Step 9: Check Gold output
echo "Checking Gold output..."
docker compose exec airflow-scheduler find /opt/airflow/data/gold -type f

echo "=============================="
echo "PIPELINE TEST COMPLETE"
echo "=============================="