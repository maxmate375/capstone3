"""
quality_checks.py - reusable data quality and validation rules.

Every check returns a structured result:
    {"rule", "layer", "status" (PASS/FAIL), "hard", "metric_value", "threshold", "sample_failing_records"}

A failed HARD rule stops the pipeline (the Job task fails, and the agents end the run).
Thresholds and the hard/soft flag for each rule are in config.QUALITY_RULES.

Used by the pipeline (Job task "quality") and by the agents.
Run as a Databricks Job task (Python script) with: --layers bronze,silver
"""

import os
import sys

if __name__ == "__main__":   # Job task: make "from src import ..." work
    script = globals().get("__file__") or sys.argv[0]   # Databricks script tasks do not define __file__
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(script))))

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.etl_pipeline import clean

SAMPLE_COLUMNS = ["booking_id", "booking_date", "booking_status", "vehicle_type",
                  "booking_value", "ride_distance", "driver_rating", "customer_rating", "payment_method"]


def result(rule, layer, metric_value, sample=None):
    """Build the structured result, comparing the metric with the rule's threshold."""
    threshold, hard = config.QUALITY_RULES[rule]
    passed = metric_value <= threshold
    return {"rule": rule, "layer": layer, "status": "PASS" if passed else "FAIL", "hard": hard,
            "metric_value": round(float(metric_value), 6), "threshold": threshold,
            "sample_failing_records": [] if passed else (sample or [])}


def share_failing(df: DataFrame, total: int, rule: str, layer: str, condition):
    """Rule = share of rows matching `condition` must stay within the threshold."""
    failing = df.filter(F.coalesce(condition, F.lit(False)))
    n = failing.count()
    cols = [c for c in SAMPLE_COLUMNS if c in df.columns]
    sample = [{k: str(v) for k, v in r.asDict().items()} for r in failing.select(cols).limit(config.SAMPLE_SIZE).collect()]
    return result(rule, layer, n / total if total else 0.0, sample)


def any_of(conditions):
    combined = F.lit(False)
    for c in conditions:
        combined = combined | F.coalesce(c, F.lit(False))
    return combined


def missing_days(df: DataFrame, start=config.EXPECTED_START_DATE, end=config.EXPECTED_END_DATE) -> list:
    expected = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
    present = {r[0] for r in df.filter(F.col("booking_date").between(start, end))
               .select("booking_date").distinct().collect()}
    days = [date.fromordinal(date.fromisoformat(start).toordinal() + i) for i in range(expected)]
    return [str(d) for d in days if d not in present]


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------
def bronze_checks(spark: SparkSession, bronze: DataFrame, previous_bronze_rows=None) -> list:
    df = clean(bronze).cache()          # typed view of Bronze so numeric rules can be evaluated
    total = df.count()
    status = F.col("booking_status")
    completed = df.filter(status == "Completed")
    n_completed = completed.count()

    checks = [
        # completeness
        share_failing(df, total, "completeness_key_columns", "bronze",
                      any_of([F.col(c).isNull() for c in config.KEY_COLUMNS])),
        # outcome completeness
        *[share_failing(completed, n_completed, f"completed_rides_missing_{c}", "bronze", F.col(c).isNull())
          for c in config.COMPLETED_REQUIRED],
        # validity
        share_failing(df, total, "validity_ratings_1_to_5", "bronze", any_of([
            ~F.col(c).between(config.RATING_MIN, config.RATING_MAX) for c in ["driver_rating", "customer_rating"]])),
        share_failing(df, total, "validity_non_negative_values", "bronze", any_of([
            F.col(c) < 0 for c in ["booking_value", "ride_distance", "avg_vtat", "avg_ctat"]])),
        share_failing(df, total, "validity_allowed_values", "bronze", any_of([
            ~status.isin(config.ALLOWED_STATUSES),
            ~F.col("vehicle_type").isin(config.ALLOWED_VEHICLE_TYPES),
            F.col("payment_method").isNotNull() & ~F.col("payment_method").isin(config.ALLOWED_PAYMENT_METHODS)])),
        # consistency
        share_failing(df, total, "consistency_flag_matches_status", "bronze", any_of([
            F.coalesce(F.col(flag) == 1, F.lit(False)) != (status == s) for s, flag in config.STATUS_FLAG.items()])),
        share_failing(df, total, "consistency_reason_present_with_flag", "bronze", any_of([
            F.col(flag).isNull() != F.col(reason).isNull() for flag, reason in config.FLAG_REASON.items()])),
    ]

    # uniqueness (the source has a few Booking ID collisions; Silver removes them)
    extra_rows = total - df.select("booking_id").distinct().count()
    dup_ids = [{"booking_id": r[0]} for r in df.groupBy("booking_id").count().filter("count > 1")
               .limit(config.SAMPLE_SIZE).collect()]
    checks.append(result("uniqueness_booking_id_bronze", "bronze", extra_rows / total if total else 0, dup_ids))

    # timeliness: data for every expected date
    gaps = missing_days(df)
    checks.append(result("timeliness_missing_days", "bronze", len(gaps),
                         [{"missing_date": d} for d in gaps[:config.SAMPLE_SIZE]]))

    # volume: drop versus the previous load
    drop = 0.0 if not previous_bronze_rows else (previous_bronze_rows - total) / previous_bronze_rows
    checks.append(result("volume_vs_previous_load", "bronze", max(drop, 0.0),
                         [{"previous_rows": previous_bronze_rows, "current_rows": total}]))
    df.unpersist()
    return checks


def previous_bronze_row_count(spark: SparkSession):
    """Row count of the Bronze table before its latest write (Delta time travel); None if first load."""
    latest = spark.sql(f"DESCRIBE HISTORY delta.`{config.BRONZE_PATH}` LIMIT 1").first()["version"]
    if latest == 0:
        return None
    return spark.read.format("delta").option("versionAsOf", latest - 1).load(config.BRONZE_PATH).count()


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------
def silver_checks(bronze: DataFrame, silver: DataFrame, quarantine: DataFrame) -> list:
    s = silver.count()
    dup_rows = s - silver.select("booking_id").distinct().count()
    b, q = bronze.count(), quarantine.count()
    return [
        result("uniqueness_booking_id_silver", "silver", dup_rows / s if s else 0),
        result("reconciliation_bronze_silver", "silver", abs(b - (s + q)),
               [{"bronze": b, "silver": s, "quarantine": q}]),
    ]


# ---------------------------------------------------------------------------
# Gold (used by the analytics agent)
# ---------------------------------------------------------------------------
def gold_checks(spark: SparkSession, previous_completion_rate=None) -> list:
    silver = spark.read.format("delta").load(config.SILVER_PATH)
    daily = spark.read.format("delta").load(config.GOLD_TABLES["daily_bookings"])
    totals = daily.agg(F.sum("total_bookings").alias("b"), F.sum("completed").alias("c")).first()
    rate = (totals["c"] or 0) / (totals["b"] or 1)
    change = 0.0 if previous_completion_rate is None else abs(rate - previous_completion_rate)
    gaps = missing_days(daily)
    return [
        result("gold_totals_reconcile_silver", "gold", abs((totals["b"] or 0) - silver.count())),
        result("gold_no_missing_dates", "gold", len(gaps), [{"missing_date": d} for d in gaps[:config.SAMPLE_SIZE]]),
        {**result("gold_rates_plausible_vs_history", "gold", change,
                  [{"completion_rate": round(rate, 4), "previous": previous_completion_rate}]),
         "completion_rate": round(rate, 4)},
    ]


# ---------------------------------------------------------------------------
# Run checks for a set of layers
# ---------------------------------------------------------------------------
def summarise(checks: list) -> dict:
    failed = [c for c in checks if c["status"] == "FAIL"]
    hard = [c["rule"] for c in failed if c["hard"]]
    return {"status": "FAIL" if hard else ("PASS_WITH_WARNINGS" if failed else "PASS"),
            "checks_passed": len(checks) - len(failed), "checks_failed": len(failed),
            "hard_failed": hard, "checks": checks}


def run_quality_checks(spark: SparkSession, layers=("bronze", "silver"), previous_completion_rate=None) -> dict:
    load = lambda path: spark.read.format("delta").load(path)
    checks = []
    if "bronze" in layers:
        checks += bronze_checks(spark, load(config.BRONZE_PATH), previous_bronze_row_count(spark))
    if "silver" in layers:
        checks += silver_checks(load(config.BRONZE_PATH), load(config.SILVER_PATH), load(config.QUARANTINE_PATH))
    if "gold" in layers:
        checks += gold_checks(spark, previous_completion_rate)
    return summarise(checks)


if __name__ == "__main__":
    import argparse
    import json
    p = argparse.ArgumentParser()
    p.add_argument("--layers", default="bronze,silver")
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    config.configure_adls(spark)
    out = run_quality_checks(spark, a.layers.split(","))
    print(json.dumps(out, indent=2, default=str))
    if out["hard_failed"]:
        raise SystemExit(f"Hard quality rules failed: {out['hard_failed']}")   # fails the Job task -> alert
