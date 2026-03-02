# TaxiOps Project: NYC Yellow Taxi Trip Analysis Pipeline

## Overview
Our Data Engineering project implements a real-time streaming pipeline that simulates NYC Yellow Taxi trip events using Faker, ingests the data through Kafka, processes it with PySpark, and performs scheduled batch aggregations using Apache Airflow. The pipeline persists both raw events and aggregated analytics to a local Parquet-based data lake.

## Business Scenario
NYC TLC wants to monitor taxi activity in near real time to understand:

1. **Stream** trip events in real time. Each event includes:
    - pickup and drop-off timestamps
    - trip distance and duration
    - pickup and drop-off taxi zones
    - passenger count
    - payment type
    - fare, tip, and total amount
2. **Process** the raw events to compute key operational metrics:
    - Trip Density by Zone
    - Demand Patterns by Time of Day
    - Revenue Performance by Location
    - Trip Distance Distribution
    - Tip Rate Behavior
    - Zone-to-Zone Mobility Flow
    - Trip Duration & Efficiency
3. **Persist** both raw and transformed data to storage (local filesystem or S3).
4. **Orchestrate** the batch and streaming jobs on a daily schedule with retry and alerting.

## Architecture

1. Data Generation (Faker Producer)
    - Generates synthetic NYC Yellow Taxi trip events matching the TLC-style schema.
    - Publishes events as JSON messages to a Kafka topic
2. Streaming Ingestion (Kafka)
    - Kafka acts as the real-time ingestion layer.
    - Taxi trip events are produced continuously and consumed by Spark.
3. Stream Processing (Pyspark)
    - Spark reads from Kafka, applies a schema, and runs a transformation pipeline:
        - Parse Kafka JSON payload into structured columns with the expected data types.
        - Data Quality Checks:
            - Filter invalid records
            - Validate timestamps
        - Add derived fields used for analytics:
            - trip_duration_min
            - pickup_date, pickup_hour
            - tip_rate
            - fare_per_mile
            - trip_category(short, medium, long)
        - Analytics Aggregations
            - compute streaming KPIs (e.g. Trip density and Trip type distribution)
4. Storage (local Parquet data lake)
    - Bronze (raw), Silver (clean/enriched), Gold (aggregated KPIs)
    - Partitioned by pickup_date
5. Orchestration (Airflow)
    - The streaming job is designed to run continuously.
    - Airflow schedules daily batch roll-ups
    - DAG includes retries and failure alerting for reliability.

## Schema
```json
{
  "event_id": "a3f9c2d1-7b4e-4c1a-9f2a-6b8e91d2f001",
  "vendor_id": 2,
  "pickup_datetime": "2023-01-01T00:25:04Z",
  "dropoff_datetime": "2023-01-01T00:37:49Z",
  "passenger_count": 1,
  "trip_distance": 2.51,
  "ratecode_id": 1,
  "store_and_fwd_flag": "N",
  "pu_location_id": 48,
  "do_location_id": 238,
  "payment_type": 1,
  "fare_amount": 14.9,
  "extra": 1.0,
  "mta_tax": 0.5,
  "tip_amount": 15.0,
  "tolls_amount": 0.0,
  "improvement_surcharge": 1.0,
  "congestion_surcharge": 2.5,
  "airport_fee": 0.0,
  "total_amount": 34.9,
  "event_timestamp": "2023-01-01T00:25:10Z"
}

Note: event_id is generated at ingestion for traceability and is not part of the original NYC TLC dataset
    