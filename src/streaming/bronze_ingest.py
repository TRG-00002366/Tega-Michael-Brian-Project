import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

TOPIC = "nyc_taxi_trips"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

DATA_DIR = os.getenv("DATA_DIR", os.path.abspath("data"))
BRONZE_PATH = os.path.join(DATA_DIR, "bronze")
CHECKPOINT_PATH = os.path.join(DATA_DIR, "checkpoints", "bronze")


def build_spark() -> SparkSession:
    """Create and configure a local Spark session."""
    spark = (
        SparkSession.builder
        .appName("TaxiOps-Bronze-Ingest")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_taxi_schema() -> StructType:
    """Return the expected taxi event schema."""
    return StructType([
        StructField("schema_version", StringType(), True),
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze ingest from Kafka to Parquet")
    parser.add_argument("--run-seconds", type=int, default=90, help="How long to keep the streaming query alive")
    args = parser.parse_args()

    spark = build_spark()
    taxi_schema = get_taxi_schema()

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )

    # Keep Kafka metadata and raw payload for lineage/replay/debugging.
    bronze_df = (
        kafka_df
        .select(
            col("topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_json"),
        )
        .withColumn("parsed", from_json(col("raw_json"), taxi_schema))
        .select(
            "topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "kafka_key",
            "raw_json",
            "parsed.*",
        )
        .withColumn("pickup_ts_tmp", to_timestamp("pickup_datetime"))
        .withColumn("pickup_date", to_date("pickup_ts_tmp"))
        .withColumn("bronze_ingested_at", current_timestamp())
        .drop("pickup_ts_tmp")
    )

    query = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("pickup_date")
        .trigger(processingTime="10 seconds")
        .start()
    )

    try:
        query.awaitTermination(args.run_seconds)
    finally:
        if query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()