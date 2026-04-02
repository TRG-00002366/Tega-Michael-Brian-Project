#!/usr/bin/env bash

set -euo pipefail

# -----------------------------
# Config
# -----------------------------
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_CONTAINER="tega-michael-brian-project-kafka-1"
TOPIC="nyc_taxi_trips"

BRONZE_DIR="$PROJECT_ROOT/data/bronze"
CHECKPOINT_DIR="$PROJECT_ROOT/data/checkpoints"
BRONZE_CHECKPOINT_DIR="$CHECKPOINT_DIR/bronze"

SPARK_KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
BRONZE_RUN_SECONDS="90"

SNOWSQL_CONNECTION="dev"

# Producer config
PRODUCER_CMD="python3 src/producer/faker_producer.py --num-events 100 --sleep-seconds 0.05"


# Helpers
# -----------------------------
log() {
  echo
  echo "============================================================"
  echo "$1"
  echo "============================================================"
}

pause_step() {
  echo
  read -rp "Press Enter to continue..." _
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: Required command '$1' is not installed."
    exit 1
  fi
}

check_container_running() {
  if ! docker ps --format '{{.Names}}' | grep -Fxq "$KAFKA_CONTAINER"; then
    echo "ERROR: Kafka container not running."
    exit 1
  fi
}


# Stages
# -----------------------------
stage_reset() {
  log "Stage 1: Reset local data + Kafka topic"

  rm -rf "$BRONZE_DIR" "$CHECKPOINT_DIR"
  mkdir -p "$BRONZE_DIR" "$BRONZE_CHECKPOINT_DIR"

  echo "Reset local storage"

  set +e
  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server localhost:9092 \
    --delete \
    --topic "$TOPIC"
  set -e

  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions 1 \
    --replication-factor 1
}

stage_produce() {
  log "Stage 2: Producing taxi events into Kafka"

  echo "Running:"
  echo "$PRODUCER_CMD"
  echo

  eval "$PRODUCER_CMD"

  echo "Finished producing events"
}

stage_bronze() {
  log "Stage 3: Bronze ingestion (Kafka to Parquet)"

  spark-submit \
    --packages "$SPARK_KAFKA_PACKAGE" \
    src/streaming/bronze_ingest.py \
    --run-seconds "$BRONZE_RUN_SECONDS"

  echo
  echo "Bronze files created:"
  find "$BRONZE_DIR" -maxdepth 3 -type f
}

stage_silver() {
  log "Stage 4: Silver transformation"

  spark-submit src/streaming/silver_transform.py
}

stage_snowflake() {
  log "Stage 5: Load to Snowflake"

  snowsql -c "$SNOWSQL_CONNECTION" -f snowflake/1_setup.sql
  snowsql -c "$SNOWSQL_CONNECTION" -f snowflake/2_load_silver.sql
}

stage_dbt() {
  log "Stage 6: Build KPI marts with dbt"

  cd dbt_taxiops
  dbt run --select stg_taxi_trips_silver hourly_kpis payment_kpis vendor_kpis
}


# Main
# -----------------------------
main() {
  log "Pipeline Demo Starting"

  require_command docker
  require_command spark-submit
  require_command snowsql
  require_command dbt
  require_command python3

  check_container_running

  echo
  echo "Pipeline stages:"
  echo "1. Reset environment"
  echo "2. Produce Kafka events"
  echo "3. Bronze ingestion"
  echo "4. Silver transform"
  echo "5. Snowflake load"
  echo "6. dbt KPIs"
  pause_step

  stage_reset
  pause_step

  stage_produce
  pause_step

  stage_bronze
  pause_step

  stage_silver
  pause_step

  stage_snowflake
  pause_step

  stage_dbt
  pause_step

  log "Pipeline completed successfully"
}

main "$@"