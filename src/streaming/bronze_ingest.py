import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
import argparse

TOPIC = "nyc_taxi_trips"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"

DATA_DIR = Path(os.getenv("DATA_DIR", str(DEFAULT_DATA_DIR)))
BRONZE_PATH = DATA_DIR / "bronze"
CHECKPOINT_PATH = DATA_DIR / "checkpoints" / "bronze"

def build_spark() -> SparkSession:

<<<<<<< HEAD
    packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:4.1.1"
=======
    packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
>>>>>>> f3e375fdc3c3635f4fbf49aa2b105460e39a6af7

    spark = (
        SparkSession.builder
        .appName("TaxiOps-Bronze-Ingest")
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.timestampFormat", "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():

    parser = argparse.ArgumentParser(description="Bronze ingest from Kafka to Parquet")
    parser.add_argument("--run-seconds", type=int, default=90, help="How long to keep the streaming query alive")
    args = parser.parse_args()

    spark = build_spark()

    # Schema expected inside the Kafka JSON payload
    taxi_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("ratecode_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pu_location_id", IntegerType(), True),
        StructField("do_location_id", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("event_timestamp", StringType(), True),
    ])

    # Read Kafka stream
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse Kafka JSON -> structured columns
    bronze_df = (
        kafka_df
        .select(col("value").cast("string").alias("json_str"))
        .select(from_json(col("json_str"), taxi_schema).alias("r"))
        .select("r.*")
        # Add partition column for the data lake
        .withColumn("pickup_ts", to_timestamp(col("pickup_datetime")))
        .withColumn("pickup_date", to_date(col("pickup_ts")))
        .drop("pickup_ts")
    )
    # Saves raw event to Bronze layer
    query = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("pickup_date")
        .trigger(processingTime="1 minute")
        .start()
    )

    try:
        query.awaitTermination(args.run_seconds)
    finally:
        if query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    os.makedirs(BRONZE_PATH, exist_ok=True)
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)
    main()