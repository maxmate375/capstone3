"""
etl_pipeline.py - ingest raw data to Bronze, then clean and transform to Silver.

Run as a Databricks Job task (Python script) with:
    --layer bronze|silver  --mode full|incremental  [--start-date yyyy-MM-dd --end-date yyyy-MM-dd]
"""

import os
import sys

if __name__ == "__main__":   # Job task: make "from src import ..." work
    script = globals().get("__file__") or sys.argv[0]   # Databricks script tasks do not define __file__
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(script))))

import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src import config

NUMERIC_COLUMNS = list(config.SILVER_TYPES)


def new_batch_id() -> str:
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def raw_schema() -> StructType:
    """Explicit 21-column schema. Bronze keeps the raw values as strings; Silver casts them."""
    return StructType([StructField(name, StringType(), True) for name in config.SOURCE_COLUMN_NAMES])


def parse_date(col_name: str = "date"):
    """Date text -> date, trying each format in config.DATE_FORMATS (invalid -> null)."""
    parsed = [F.expr(f"try_to_timestamp(`{col_name}`, '{fmt}')") for fmt in config.DATE_FORMATS]
    patterns = [r"^\d{2}-\d{2}-\d{4}$" if fmt == "dd-MM-yyyy" else r"^\d{4}-\d{2}-\d{2}$"
                for fmt in config.DATE_FORMATS]
    return F.coalesce(*[F.when(F.col(col_name).rlike(p), d) for p, d in zip(patterns, parsed)]).cast("date")


def write_delta(df: DataFrame, path: str, mode: str, start_date=None, end_date=None):
    """
    Idempotent write.
    full        -> overwrite the table
    incremental -> replace only the booking dates being loaded (replaceWhere), so re-running
                   the same dates never creates duplicates
    """
    from delta.tables import DeltaTable
    writer = df.write.format("delta").mode("overwrite")
    if mode == "incremental" and DeltaTable.isDeltaTable(df.sparkSession, path):
        writer = writer.option("replaceWhere", f"booking_date BETWEEN '{start_date}' AND '{end_date}'")
    else:
        writer = writer.option("overwriteSchema", "true")
    writer.save(path)


# ===========================================================================
# BRONZE
# ===========================================================================
def read_raw(spark: SparkSession, path: str = None) -> DataFrame:
    """Read the raw CSV files with the explicit schema and record each row's source file."""
    reader = spark.read.schema(raw_schema()).options(**config.CSV_OPTIONS)
    return reader.csv(path or config.RAW_PATH).select("*", F.col("_metadata.file_path").alias("source_file"))


def to_bronze(raw: DataFrame, batch_id: str) -> DataFrame:
    """Rename to snake_case, strip quotes from the IDs, add ingestion metadata."""
    df = raw
    for src, dst in config.SOURCE_COLUMNS:
        df = df.withColumnRenamed(src, dst)
    for id_col in ["booking_id", "customer_id"]:
        cleaned = F.trim(F.regexp_replace(id_col, '"', ""))
        df = df.withColumn(id_col, F.when(cleaned == "", None).otherwise(cleaned))
    return (df.withColumn("booking_date", parse_date("date"))      # used for incremental loads
              .withColumn("ingestion_timestamp", F.current_timestamp())
              .withColumn("batch_id", F.lit(batch_id)))


def check_header(spark: SparkSession, path: str = None):
    """Fail fast if the source files do not have the expected 21 columns (schema change)."""
    header = spark.read.option("header", "true").csv(path or config.RAW_PATH).columns
    if header != config.SOURCE_COLUMN_NAMES:
        missing = [c for c in config.SOURCE_COLUMN_NAMES if c not in header]
        extra = [c for c in header if c not in config.SOURCE_COLUMN_NAMES]
        raise ValueError(f"Schema change in raw files. Missing columns: {missing}. Unexpected: {extra}")


def run_bronze(spark: SparkSession, mode="full", start_date=None, end_date=None, batch_id=None) -> dict:
    """Raw CSV -> Bronze Delta table."""
    if mode not in ("full", "incremental"):
        raise ValueError(f"mode must be 'full' or 'incremental', got {mode!r}")
    batch_id = batch_id or new_batch_id()
    check_header(spark)
    bronze = to_bronze(read_raw(spark), batch_id)
    if mode == "incremental":
        bronze = bronze.filter(F.col("booking_date").between(start_date, end_date))
    write_delta(bronze, config.BRONZE_PATH, mode, start_date, end_date)
    return {"layer": "bronze", "batch_id": batch_id, "mode": mode,
            "rows_written": spark.read.format("delta").load(config.BRONZE_PATH).count()}


# ===========================================================================
# SILVER
# ===========================================================================
def standardise(col_name: str, allowed: list):
    """Trim, collapse spaces and match allowed values ignoring case ('  completed' -> 'Completed')."""
    cleaned = F.trim(F.regexp_replace(F.col(col_name), r"\s+", " "))
    lookup = F.create_map(*[x for v in allowed for x in (F.lit(v.lower()), F.lit(v))])
    return F.coalesce(lookup[F.lower(cleaned)], cleaned)


def clean(bronze: DataFrame) -> DataFrame:
    """Standardise text, cast numeric columns and build booking_timestamp (no rows removed)."""
    df = (bronze
          .withColumn("booking_status", standardise("booking_status", config.ALLOWED_STATUSES))
          .withColumn("vehicle_type", standardise("vehicle_type", config.ALLOWED_VEHICLE_TYPES))
          .withColumn("payment_method", standardise("payment_method", config.ALLOWED_PAYMENT_METHODS)))
    for c, t in config.SILVER_TYPES.items():
        df = df.withColumn(f"raw_{c}", F.col(c)).withColumn(c, F.expr(f"try_cast(`{c}` AS {t})"))
    return (df.withColumn("booking_date", parse_date("date"))
              .withColumn("booking_timestamp", F.expr(
                  "try_to_timestamp(concat(cast(booking_date AS string), ' ', time), 'yyyy-MM-dd HH:mm:ss')")))


def reject_rules() -> list:
    """(reason, condition) for every way a record can be rejected to quarantine."""
    s = F.col("booking_status")
    rules = [(f"missing_{c}", F.col(c).isNull()) for c in config.KEY_COLUMNS]
    rules += [
        ("invalid_date_or_time", F.col("booking_timestamp").isNull()),
        ("invalid_booking_status", ~s.isin(config.ALLOWED_STATUSES)),
        ("invalid_vehicle_type", ~F.col("vehicle_type").isin(config.ALLOWED_VEHICLE_TYPES)),
        ("invalid_payment_method", F.col("payment_method").isNotNull()
         & ~F.col("payment_method").isin(config.ALLOWED_PAYMENT_METHODS)),
        ("negative_booking_value", F.col("booking_value") < 0),
        ("negative_ride_distance", F.col("ride_distance") < 0),
        ("driver_rating_outside_1_to_5", ~F.col("driver_rating").between(config.RATING_MIN, config.RATING_MAX)),
        ("customer_rating_outside_1_to_5", ~F.col("customer_rating").between(config.RATING_MIN, config.RATING_MAX)),
    ]
    rules += [(f"not_a_number_{c}", F.col(f"raw_{c}").isNotNull() & F.col(c).isNull()) for c in NUMERIC_COLUMNS]
    rules += [(f"completed_without_{c}", (s == "Completed") & F.col(c).isNull()) for c in config.COMPLETED_REQUIRED]
    for status, flag in config.STATUS_FLAG.items():
        rules.append((f"{flag}_does_not_match_status", F.coalesce(F.col(flag) == 1, F.lit(False)) != (s == status)))
        rules.append((f"{config.FLAG_REASON[flag]}_without_flag",
                      F.col(flag).isNull() != F.col(config.FLAG_REASON[flag]).isNull()))
    return rules


def to_silver(bronze: DataFrame, batch_id: str):
    """Return (silver, quarantine)."""
    df = clean(bronze)
    # every rule a row breaks (a null condition counts as not broken)
    reasons = F.filter(F.array(*[F.when(F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in reject_rules()]),
                       lambda x: x.isNotNull())
    df = df.withColumn("reject_reasons", reasons)

    # remove duplicate Booking IDs among valid rows: keep the earliest booking
    valid = F.size("reject_reasons") == 0
    earliest_first = Window.partitionBy("booking_id", valid).orderBy("booking_timestamp", "source_file")
    df = (df.withColumn("_rank", F.when(valid, F.row_number().over(earliest_first)))
            .withColumn("reject_reasons", F.when(F.col("_rank") > 1, F.array(F.lit("duplicate_booking_id")))
                                           .otherwise(F.col("reject_reasons")))
            .drop("_rank"))

    status = F.col("booking_status")
    silver = (df.filter(F.size("reject_reasons") == 0)
                .withColumn("booking_hour", F.hour("booking_timestamp"))
                .withColumn("day_of_week", F.date_format("booking_date", "EEEE"))
                .withColumn("is_cancelled", status.isin("Cancelled by Customer", "Cancelled by Driver"))
                .withColumn("cancelled_by", F.when(status == "Cancelled by Customer", "customer")
                                             .when(status == "Cancelled by Driver", "driver"))
                .withColumn("cancellation_reason", F.when(status == "Cancelled by Customer", F.col("customer_cancel_reason"))
                                                    .when(status == "Cancelled by Driver", F.col("driver_cancel_reason")))
                .drop("date", "time", "reject_reasons", *[f"raw_{c}" for c in NUMERIC_COLUMNS])
                .withColumn("silver_batch_id", F.lit(batch_id)))

    # quarantine keeps the original values as they arrived, plus the reasons
    quarantine = (df.filter(F.size("reject_reasons") > 0)
                    .select(*[F.col(f"raw_{c}").alias(c) if c in NUMERIC_COLUMNS else F.col(c) for c in bronze.columns],
                            F.array_join("reject_reasons", ",").alias("reject_reason"))
                    .withColumn("quarantined_at", F.current_timestamp()))
    return silver, quarantine


def run_silver(spark: SparkSession, mode="full", start_date=None, end_date=None, batch_id=None) -> dict:
    """Bronze -> Silver + Quarantine."""
    batch_id = batch_id or new_batch_id()
    bronze = spark.read.format("delta").load(config.BRONZE_PATH)
    if mode == "incremental":
        bronze = bronze.filter(F.col("booking_date").between(start_date, end_date))
    silver, quarantine = to_silver(bronze, batch_id)
    write_delta(silver, config.SILVER_PATH, mode, start_date, end_date)
    write_delta(quarantine, config.QUARANTINE_PATH, mode, start_date, end_date)
    return {"layer": "silver", "batch_id": batch_id, "mode": mode,
            "silver_rows": spark.read.format("delta").load(config.SILVER_PATH).count(),
            "quarantine_rows": spark.read.format("delta").load(config.QUARANTINE_PATH).count()}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--layer", choices=["bronze", "silver"], required=True)
    p.add_argument("--mode", default="full")
    p.add_argument("--start-date", default="")
    p.add_argument("--end-date", default="")
    p.add_argument("--batch-id", default="")
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    config.configure_adls(spark)
    run = run_bronze if a.layer == "bronze" else run_silver
    print(run(spark, a.mode, a.start_date or None, a.end_date or None, a.batch_id or None))
