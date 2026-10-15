"""
gold_analytics.py - build business-ready Gold tables from Silver.

Revenue = Booking Value of Completed rides.
Run as a Databricks Job task (Python script) with no arguments.
"""

import os
import sys

if __name__ == "__main__":   # Job task: make "from src import ..." work
    script = globals().get("__file__") or sys.argv[0]   # Databricks script tasks do not define __file__
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(script))))

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src import config


def daily_bookings(silver: DataFrame) -> DataFrame:
    completed = F.col("booking_status") == "Completed"
    return (silver.groupBy("booking_date")
            .agg(F.count("*").alias("total_bookings"),
                 F.sum(completed.cast("int")).alias("completed"),
                 F.sum(F.col("is_cancelled").cast("int")).alias("cancelled"),
                 F.round(F.sum(F.when(completed, F.col("booking_value"))), 2).alias("revenue"))
            .withColumn("completion_rate", F.round(F.col("completed") / F.col("total_bookings"), 4))
            .withColumn("booking_month", F.date_format("booking_date", "yyyy-MM")))


def cancellations_by_reason(silver: DataFrame) -> DataFrame:
    return (silver.filter("is_cancelled")
            .groupBy("cancelled_by", "cancellation_reason")
            .agg(F.count("*").alias("cancellations")))


def revenue_by_vehicle_payment(silver: DataFrame) -> DataFrame:
    return (silver.filter(F.col("booking_status") == "Completed")
            .groupBy("vehicle_type", "payment_method")
            .agg(F.count("*").alias("completed_rides"),
                 F.round(F.sum("booking_value"), 2).alias("revenue"),
                 F.round(F.avg("booking_value"), 2).alias("avg_booking_value")))


def peak_hour_demand(silver: DataFrame) -> DataFrame:
    return silver.groupBy("booking_hour", "day_of_week").agg(F.count("*").alias("bookings"))


def top_locations(silver: DataFrame, n: int = 10) -> DataFrame:
    def top(col_name, kind):
        rank = Window.orderBy(F.desc("bookings"))
        return (silver.groupBy(F.col(col_name).alias("location")).agg(F.count("*").alias("bookings"))
                .withColumn("rank", F.row_number().over(rank)).filter(F.col("rank") <= n)
                .withColumn("location_type", F.lit(kind)))
    return top("pickup_location", "pickup").unionByName(top("drop_location", "drop"))


def tat_by_vehicle(silver: DataFrame) -> DataFrame:
    return (silver.groupBy("vehicle_type")
            .agg(F.round(F.avg("avg_vtat"), 2).alias("avg_vtat_minutes"),
                 F.round(F.avg("avg_ctat"), 2).alias("avg_ctat_minutes")))


def ratings_by_vehicle(silver: DataFrame) -> DataFrame:
    return (silver.groupBy("vehicle_type")
            .agg(F.round(F.avg("driver_rating"), 2).alias("avg_driver_rating"),
                 F.round(F.avg("customer_rating"), 2).alias("avg_customer_rating")))


def incomplete_by_reason(silver: DataFrame) -> DataFrame:
    return (silver.filter(F.col("booking_status") == "Incomplete")
            .groupBy("incomplete_reason").agg(F.count("*").alias("incomplete_rides")))


BUILDERS = {
    "daily_bookings": daily_bookings,
    "cancellations_by_reason": cancellations_by_reason,
    "revenue_by_vehicle_payment": revenue_by_vehicle_payment,
    "peak_hour_demand": peak_hour_demand,
    "top_locations": top_locations,
    "tat_by_vehicle": tat_by_vehicle,
    "ratings_by_vehicle": ratings_by_vehicle,
    "incomplete_by_reason": incomplete_by_reason,
}


def run_gold(spark: SparkSession) -> dict:
    """Rebuild every Gold table from Silver, then optimise."""
    silver = spark.read.format("delta").load(config.SILVER_PATH)
    rows = {}
    for name, build in BUILDERS.items():
        writer = build(silver).write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if name == "daily_bookings":
            writer = writer.partitionBy("booking_month")          # partitioning
        writer.save(config.GOLD_TABLES[name])
        rows[name] = spark.read.format("delta").load(config.GOLD_TABLES[name]).count()

    # optimisation: compact small files; Z-order where tables are filtered by date / vehicle
    spark.sql(f"OPTIMIZE delta.`{config.GOLD_TABLES['daily_bookings']}` ZORDER BY (booking_date)")
    spark.sql(f"OPTIMIZE delta.`{config.SILVER_PATH}` ZORDER BY (booking_date, vehicle_type)")
    return {"layer": "gold", "tables": rows}


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    config.configure_adls(spark)
    print(run_gold(spark))
