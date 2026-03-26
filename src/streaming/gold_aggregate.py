import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, when, round, unix_timestamp, count, sum, avg, max

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
SILVER_PATH = os.path.join(DATA_DIR, "silver")
GOLD_PATH = os.path.join(DATA_DIR, "gold")

HOURLY_KPI_PATH = os.path.join(GOLD_PATH, "hourly_kpis")
PAYMENT_KPI_PATH = os.path.join(GOLD_PATH, "payment_kpis")
VENDOR_KPI_PATH = os.path.join(GOLD_PATH, "vendor_kpis")


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("TaxiOps-Gold-Aggregate")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    print("starting gold aggregation")
    spark = build_spark()

    silver_df = spark.read.parquet(SILVER_PATH)

    gold_base_df = (
        silver_df
        .withColumn("pickup_date", to_date(col("pickup_ts")))
        .withColumn("pickup_hour", hour(col("pickup_ts")))
        .withColumn(
            "trip_duration_min",
            round(
                (unix_timestamp(col("dropoff_ts")) - unix_timestamp(col("pickup_ts"))) / 60.0,
                2
            )
        )
        .withColumn(
            "tip_rate",
            when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount")).otherwise(0.0)
        )
        .withColumn(
            "fare_per_mile",
            when(col("trip_distance") > 0, col("fare_amount") / col("trip_distance")).otherwise(0.0)
        )
    )

    # Hourly KPI table
    hourly_kpis_df = (
        gold_base_df
        .groupBy("pickup_date", "pickup_hour")
        .agg(
            count("*").alias("total_trips"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(avg("fare_amount"), 2).alias("avg_fare_amount"),
            round(avg("tip_amount"), 2).alias("avg_tip_amount"),
            round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            round(avg("trip_duration_min"), 2).alias("avg_trip_duration_min")
        )
        .orderBy("pickup_date", "pickup_hour")
    )

    # Payment type KPI table
    payment_kpis_df = (
        gold_base_df
        .groupBy("pickup_date", "payment_type")
        .agg(
            count("*").alias("total_trips"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(avg("fare_amount"), 2).alias("avg_fare_amount"),
            round(avg("tip_rate"), 4).alias("avg_tip_rate")
        )
        .orderBy("pickup_date", "payment_type")
    )

    # Vendor KPI table
    vendor_kpis_df = (
        gold_base_df
        .groupBy("pickup_date", "vendor_id")
        .agg(
            count("*").alias("total_trips"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            round(avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
            round(max("total_amount"), 2).alias("max_trip_total")
        )
        .orderBy("pickup_date", "vendor_id")
    )

    hourly_kpis_df.write.mode("overwrite").partitionBy("pickup_date").parquet(HOURLY_KPI_PATH)
    payment_kpis_df.write.mode("overwrite").partitionBy("pickup_date").parquet(PAYMENT_KPI_PATH)
    vendor_kpis_df.write.mode("overwrite").partitionBy("pickup_date").parquet(VENDOR_KPI_PATH)

    hourly_kpis_df.show(10, truncate=False)
    payment_kpis_df.show(10, truncate=False)
    vendor_kpis_df.show(10, truncate=False)

    print("gold aggregation finished")


if __name__ == "__main__":
    main()