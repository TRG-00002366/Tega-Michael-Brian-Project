import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DATA_DIR = os.getenv("DATA_DIR", os.path.abspath("data"))
BRONZE_PATH = os.path.join(DATA_DIR, "bronze")
SILVER_PATH = os.path.join(DATA_DIR, "silver")

ISO_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"
ISO_TS_FRACTION_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"

MIN_VALID_DATE = "2025-01-01"
MAX_VALID_DATE = "2030-12-31"


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("TaxiOps-Silver-Transform")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    spark = build_spark()

    bronze_df = spark.read.parquet(BRONZE_PATH)

    silver_base_df = (
        bronze_df
        .withColumn("pickup_ts", F.to_timestamp(F.col("pickup_datetime"), ISO_TS_FORMAT))
        .withColumn("dropoff_ts", F.to_timestamp(F.col("dropoff_datetime"), ISO_TS_FORMAT))
        .withColumn("event_ts", F.to_timestamp(F.col("event_timestamp"), ISO_TS_FRACTION_FORMAT))
        .withColumnRenamed("pu_location_id", "pickup_location_id")
        .withColumnRenamed("do_location_id", "dropoff_location_id")
    )

    dedupe_window = Window.partitionBy("event_id").orderBy(F.col("kafka_offset").desc())

    silver_deduped_df = (
        silver_base_df
        .withColumn("row_num", F.row_number().over(dedupe_window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    silver_df = (
        silver_deduped_df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("dropoff_ts").isNotNull())
        .filter(F.col("dropoff_ts") > F.col("pickup_ts"))
        .filter(F.col("passenger_count").between(1, 6))
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_distance") <= 100)
        .filter(F.col("fare_amount") >= 0)
        .filter(F.col("tip_amount") >= 0)
        .filter(F.col("total_amount") >= 0)
        .filter(F.col("vendor_id").isin(1, 2))
        .filter(F.col("payment_type").isin(1, 2, 3, 4))
        .filter(F.col("pickup_location_id").between(1, 263))
        .filter(F.col("dropoff_location_id").between(1, 263))
        .withColumn("pickup_date", F.to_date(F.col("pickup_ts")))
        .withColumn("pickup_hour", F.hour(F.col("pickup_ts")))
        .withColumn(
            "trip_duration_min",
            F.round((F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0, 2)
        )
        .withColumn(
            "trip_speed_mph",
            F.when(
                F.col("trip_distance") > 0,
                F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60.0), 2)
            )
        )
        .withColumn(
            "fare_per_mile",
            F.when(
                F.col("trip_distance") > 0,
                F.round(F.col("fare_amount") / F.col("trip_distance"), 2)
            )
        )
        .withColumn(
            "tip_rate",
            F.when(
                F.col("fare_amount") > 0,
                F.round(F.col("tip_amount") / F.col("fare_amount"), 4)
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "is_airport_trip",
            F.when(
                F.col("pickup_location_id").isin(132, 138) |
                F.col("dropoff_location_id").isin(132, 138),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn(
            "trip_time_bucket",
            F.when(F.col("pickup_hour").between(6, 10), "morning")
             .when(F.col("pickup_hour").between(11, 15), "midday")
             .when(F.col("pickup_hour").between(16, 20), "evening")
             .otherwise("overnight")
        )
        .filter(F.col("pickup_date").isNotNull())
        .filter(F.col("pickup_date").between(MIN_VALID_DATE, MAX_VALID_DATE))
        .filter(F.col("trip_duration_min") > 0)
        .filter(F.col("trip_duration_min") <= 300)
        .filter((F.col("trip_speed_mph").isNull()) | (F.col("trip_speed_mph") <= 80))
        # Convert timestamps to strings before writing to Parquet for Snowflake
        .withColumn("pickup_ts_str", F.date_format(F.col("pickup_ts"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("dropoff_ts_str", F.date_format(F.col("dropoff_ts"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("event_ts_str", F.date_format(F.col("event_ts"), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
        .select(
            "event_id",
            "schema_version",
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            F.col("pickup_ts_str").alias("pickup_ts"),
            F.col("dropoff_ts_str").alias("dropoff_ts"),
            F.col("event_ts_str").alias("event_ts"),
            "pickup_date",
            "pickup_hour",
            "passenger_count",
            "trip_distance",
            "trip_duration_min",
            "trip_speed_mph",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "fare_per_mile",
            "tip_rate",
            "payment_type",
            "ratecode_id",
            "store_and_fwd_flag",
            "pickup_location_id",
            "dropoff_location_id",
            "extra",
            "mta_tax",
            "tolls_amount",
            "improvement_surcharge",
            "congestion_surcharge",
            "airport_fee",
            "is_airport_trip",
            "trip_time_bucket",
            "topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "bronze_ingested_at",
        )
    )

    print("Silver timestamp/date sanity check:")
    silver_df.selectExpr(
        "min(pickup_ts) as min_pickup_ts",
        "max(pickup_ts) as max_pickup_ts",
        "min(pickup_date) as min_pickup_date",
        "max(pickup_date) as max_pickup_date"
    ).show(truncate=False)

    (
        silver_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )

    silver_df.printSchema()
    silver_df.show(10, truncate=False)
    print("Silver transformation finished.")

    spark.stop()


if __name__ == "__main__":
    main()