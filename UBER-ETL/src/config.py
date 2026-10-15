"""
config.py - central configuration for uber-taxi-agentic-etl.

All paths, thresholds and settings live here so no value is hard-coded in pipeline code.
Values come from environment variables when set, otherwise from the defaults below.
Secrets are never stored here: they are read at runtime from the Databricks secret scope
(backed by Azure Key Vault).
"""

import os


def _env(name, default):
    return os.environ.get(name, default)


# ---------------------------------------------------------------------------
# Storage account, containers and secret scope
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT = _env("STORAGE_ACCOUNT", "stuberetlabhi01")
ADLS_HOST = f"{STORAGE_ACCOUNT}.dfs.core.windows.net"
CONTAINERS = ["raw", "bronze", "silver", "gold", "quarantine", "reports"]

SECRET_SCOPE = _env("SECRET_SCOPE", "uber-etl-scope")
SECRET_KEYS = {                       # names of the secrets in the scope (values live in Key Vault)
    "client_id": "sp-client-id",
    "tenant_id": "sp-tenant-id",
    "client_secret": "sp-client-secret",
    "foundry_endpoint": "foundry-endpoint",
    "foundry_deployment": "foundry-deployment",
    "foundry_api_key": "foundry-api-key",   # only used if FOUNDRY_AUTH_MODE = "key"
}

CATALOG = _env("CATALOG", "dbw_uber_etl")   # Unity Catalog (Hive metastore is disabled)
SCHEMA = _env("SCHEMA", "uber_etl")


def abfss(container, path=""):
    """Full ADLS path, e.g. abfss://silver@<account>.dfs.core.windows.net/uber_rides"""
    return f"abfss://{container}@{ADLS_HOST}/{path}"


# ---------------------------------------------------------------------------
# ADLS paths per layer (layout from the project spec)
# ---------------------------------------------------------------------------
RAW_PATH = abfss("raw", "uber_rides/")
BRONZE_PATH = abfss("bronze", "uber_rides")
SILVER_PATH = abfss("silver", "uber_rides")
QUARANTINE_PATH = abfss("quarantine", "uber_rides")
REPORTS_PATH = abfss("reports", "runs")

GOLD_TABLES = {name: abfss("gold", name) for name in [
    "daily_bookings",              # daily bookings and completion rate
    "cancellations_by_reason",     # customer versus driver
    "revenue_by_vehicle_payment",  # revenue by vehicle type and payment method
    "peak_hour_demand",            # demand by hour of day
    "top_locations",               # top pickup and drop locations
    "tat_by_vehicle",              # average VTAT and CTAT by vehicle type
    "ratings_by_vehicle",          # average driver and customer ratings
    "incomplete_by_reason",        # incomplete rides by reason
]}


# ---------------------------------------------------------------------------
# Source schema: 21 columns (source name -> name used from Bronze on)
# ---------------------------------------------------------------------------
SOURCE_COLUMNS = [
    ("Date", "date"),
    ("Time", "time"),
    ("Booking ID", "booking_id"),
    ("Booking Status", "booking_status"),
    ("Customer ID", "customer_id"),
    ("Vehicle Type", "vehicle_type"),
    ("Pickup Location", "pickup_location"),
    ("Drop Location", "drop_location"),
    ("Avg VTAT", "avg_vtat"),
    ("Avg CTAT", "avg_ctat"),
    ("Cancelled Rides by Customer", "cancelled_by_customer_flag"),
    ("Reason for cancelling by Customer", "customer_cancel_reason"),
    ("Cancelled Rides by Driver", "cancelled_by_driver_flag"),
    ("Driver Cancellation Reason", "driver_cancel_reason"),
    ("Incomplete Rides", "incomplete_flag"),
    ("Incomplete Rides Reason", "incomplete_reason"),
    ("Booking Value", "booking_value"),
    ("Ride Distance", "ride_distance"),
    ("Driver Ratings", "driver_rating"),
    ("Customer Rating", "customer_rating"),
    ("Payment Method", "payment_method"),
]
SOURCE_COLUMN_NAMES = [src for src, _ in SOURCE_COLUMNS]

# Silver types for the numeric columns (everything else stays string)
SILVER_TYPES = {
    "avg_vtat": "double", "avg_ctat": "double",
    "cancelled_by_customer_flag": "int", "cancelled_by_driver_flag": "int", "incomplete_flag": "int",
    "booking_value": "double", "ride_distance": "double",
    "driver_rating": "double", "customer_rating": "double",
}

CSV_OPTIONS = {
    "header": "true",
    "nullValue": "null",   # missing values are the literal text null
    "escape": '"',         # IDs are stored as """CNR5884300""" in the file
}

# The spec says dd-MM-yyyy; profiling showed the file uses yyyy-MM-dd. Both are accepted.
DATE_FORMATS = ["dd-MM-yyyy", "yyyy-MM-dd"]


# ---------------------------------------------------------------------------
# Business rules (allowed values confirmed during profiling)
# ---------------------------------------------------------------------------
ALLOWED_STATUSES = ["Completed", "Cancelled by Driver", "Cancelled by Customer", "No Driver Found", "Incomplete"]
ALLOWED_VEHICLE_TYPES = ["Auto", "Go Mini", "Go Sedan", "Bike", "Premier Sedan", "eBike", "Uber XL"]
ALLOWED_PAYMENT_METHODS = ["UPI", "Cash", "Uber Wallet", "Credit Card", "Debit Card"]

KEY_COLUMNS = ["booking_id", "date", "time", "booking_status", "customer_id",
               "vehicle_type", "pickup_location", "drop_location"]
COMPLETED_REQUIRED = ["booking_value", "ride_distance", "payment_method"]
RATING_MIN, RATING_MAX = 1.0, 5.0

# status -> flag that must be 1 for it; each flag -> its reason column
STATUS_FLAG = {
    "Cancelled by Customer": "cancelled_by_customer_flag",
    "Cancelled by Driver": "cancelled_by_driver_flag",
    "Incomplete": "incomplete_flag",
}
FLAG_REASON = {
    "cancelled_by_customer_flag": "customer_cancel_reason",
    "cancelled_by_driver_flag": "driver_cancel_reason",
    "incomplete_flag": "incomplete_reason",
}


# ---------------------------------------------------------------------------
# Quality thresholds: rule -> (threshold, hard). A failed hard rule stops the run.
# Thresholds are the share of rows allowed to fail, unless noted.
# ---------------------------------------------------------------------------
QUALITY_RULES = {
    "completeness_key_columns":               (0.0,   True),
    "completed_rides_missing_booking_value":  (0.001, True),
    "completed_rides_missing_ride_distance":  (0.001, True),
    "completed_rides_missing_payment_method": (0.001, False),
    "uniqueness_booking_id_bronze":           (0.01,  True),   # source has ~0.8% ID collisions
    "uniqueness_booking_id_silver":           (0.0,   True),
    "validity_ratings_1_to_5":                (0.01,  True),
    "validity_non_negative_values":           (0.01,  True),
    "validity_allowed_values":                (0.01,  True),
    "consistency_flag_matches_status":        (0.01,  True),
    "consistency_reason_present_with_flag":   (0.01,  True),
    "timeliness_missing_days":                (0,     True),   # number of days with no data
    "volume_vs_previous_load":                (0.20,  True),   # allowed drop vs previous load
    "reconciliation_bronze_silver":           (0,     True),   # bronze - (silver + quarantine)
    # Gold checks (used by the analytics agent)
    "gold_totals_reconcile_silver":           (0,     True),
    "gold_no_missing_dates":                  (0,     True),
    "gold_rates_plausible_vs_history":        (0.05,  False),  # completion-rate change vs last run
}
EXPECTED_START_DATE = _env("EXPECTED_START_DATE", "2024-01-01")
EXPECTED_END_DATE = _env("EXPECTED_END_DATE", "2024-12-30")
SAMPLE_SIZE = 5                     # sample failing records returned per check


# ---------------------------------------------------------------------------
# Databricks Job
# ---------------------------------------------------------------------------
JOB_NAME = _env("JOB_NAME", "uber-taxi-etl")
JOB_TIMEOUT_SECONDS = int(_env("JOB_TIMEOUT_SECONDS", "1800"))
JOB_POLL_SECONDS = 20


# ---------------------------------------------------------------------------
# Microsoft Foundry GPT (endpoint and deployment from env vars or the secret scope)
# ---------------------------------------------------------------------------
FOUNDRY_API_VERSION = _env("FOUNDRY_API_VERSION", "2024-10-21")
FOUNDRY_AUTH_MODE = _env("FOUNDRY_AUTH_MODE", "entra")   # "entra" (preferred) or "key"
LLM_TEMPERATURE = 0.0
LLM_MAX_TOKENS_PER_CALL = 800
LLM_MAX_TOKENS_PER_RUN = 20000


# ---------------------------------------------------------------------------
# Agent guardrails
# ---------------------------------------------------------------------------
AGENT_MAX_RETRIES = 2
AGENT_MAX_STEPS = 12
MAX_BACKFILL_DAYS = 31                      # larger backfills need human approval
AGENT_WRITE_PATHS = [REPORTS_PATH]          # agents may only write run reports


# ---------------------------------------------------------------------------
# Runtime helpers
# ---------------------------------------------------------------------------
def get_dbutils(spark):
    from pyspark.dbutils import DBUtils
    return DBUtils(spark)


def get_secret(spark, name):
    return get_dbutils(spark).secrets.get(scope=SECRET_SCOPE, key=SECRET_KEYS[name])


def get_setting(spark, env_name, secret_name):
    """A setting from an environment variable, else from the secret scope."""
    return os.environ.get(env_name) or get_secret(spark, secret_name)


def configure_adls(spark):
    """Let Spark read/write ADLS as the service principal (OAuth). Call once per session."""
    h = ADLS_HOST
    spark.conf.set(f"fs.azure.account.auth.type.{h}", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{h}",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{h}", get_secret(spark, "client_id"))
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{h}", get_secret(spark, "client_secret"))
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{h}",
                   f"https://login.microsoftonline.com/{get_secret(spark, 'tenant_id')}/oauth2/token")
