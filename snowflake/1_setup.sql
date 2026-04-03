CREATE OR REPLACE WAREHOUSE TAXIOPS_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE OR REPLACE DATABASE TAXIOPS_DB;
CREATE OR REPLACE SCHEMA TAXIOPS_DB.RAW;

USE WAREHOUSE TAXIOPS_WH;
USE DATABASE TAXIOPS_DB;
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT PARQUET_FMT
  TYPE = PARQUET;

CREATE OR REPLACE STAGE TAXI_SILVER_STAGE
  FILE_FORMAT = PARQUET_FMT;

CREATE OR REPLACE TABLE TAXI_TRIPS_SILVER (
  event_id STRING,
  schema_version STRING,
  vendor_id NUMBER,
  pickup_datetime STRING,
  dropoff_datetime STRING,
  pickup_ts STRING,
  dropoff_ts STRING,
  event_ts STRING,
  pickup_date DATE,
  pickup_hour NUMBER,
  passenger_count NUMBER,
  trip_distance FLOAT,
  trip_duration_min FLOAT,
  trip_speed_mph FLOAT,
  fare_amount FLOAT,
  tip_amount FLOAT,
  total_amount FLOAT,
  fare_per_mile FLOAT,
  tip_rate FLOAT,
  payment_type NUMBER,
  ratecode_id NUMBER,
  store_and_fwd_flag STRING,
  pickup_location_id NUMBER,
  dropoff_location_id NUMBER,
  extra FLOAT,
  mta_tax FLOAT,
  tolls_amount FLOAT,
  improvement_surcharge FLOAT,
  congestion_surcharge FLOAT,
  airport_fee FLOAT,
  is_airport_trip BOOLEAN,
  trip_time_bucket STRING,
  topic STRING,
  kafka_partition NUMBER,
  kafka_offset NUMBER,
  kafka_timestamp TIMESTAMP_NTZ,
  bronze_ingested_at TIMESTAMP_NTZ
);