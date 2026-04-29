"""
Shared constants for all DAGs in the Crypto Market Data Platform.

Design principle:
  Runtime-configurable thresholds are read from Airflow Variables at DAG
  parse time so operators receive the value at task execution — not at
  DAG import. This means threshold changes take effect on the next run
  without restarting the scheduler.

  Static structural constants (dataset names, model lists, tag labels)
  are defined here and imported by both DAGs and operators.
"""

from __future__ import annotations

# ─── GCP / BigQuery ───────────────────────────────────────────────────────────

GCP_PROJECT_ID: str = "vg-ind-2026"
GCP_REGION: str = "us-central1"

BQ_BRONZE_DATASET: str = "bronze_raw"
BQ_SILVER_DATASET: str = "silver_curated"
BQ_GOLD_DATASET: str = "gold_analytics"

BQ_DEAD_LETTER_TABLE: str = f"{GCP_PROJECT_ID}.{BQ_BRONZE_DATASET}.pipeline_dead_letter"
BQ_RAW_TRADES_TABLE: str = f"{GCP_PROJECT_ID}.{BQ_BRONZE_DATASET}.raw_trades"
BQ_OHLCV_TABLE: str = f"{GCP_PROJECT_ID}.{BQ_SILVER_DATASET}.ohlcv_1min"

# ─── dbt ──────────────────────────────────────────────────────────────────────

DBT_PROJECT_DIR: str = "/opt/airflow/dbt/crypto_platform"
DBT_PROFILES_DIR: str = "/opt/airflow/dbt/crypto_platform"
DBT_TARGET: str = "prod"

# Ordered Silver models — dependency-aware execution sequence
DBT_SILVER_MODELS: list[str] = [
    "silver_deduped_trades",
    "silver_ohlcv_validated",
]

# Gold models — run in parallel (no inter-Gold dependencies)
DBT_GOLD_MODELS: list[str] = [
    "gold_daily_ohlcv",
    "gold_price_stats_24h",
    "gold_trade_volume_hourly",
]

# ─── Airflow Variable Keys ────────────────────────────────────────────────────
# These keys are read from Airflow Variables at runtime.
# Set via: Airflow UI → Admin → Variables, or `airflow variables set <key> <val>`

AIRFLOW_VAR_DEAD_LETTER_MAX_PER_HOUR: str = "dead_letter_max_per_hour"
AIRFLOW_VAR_INGESTOR_HEALTH_URL: str = "ingestor_cloud_run_url"
AIRFLOW_VAR_SILVER_MIN_ROWS_PER_HOUR: str = "silver_min_rows_per_hour"
AIRFLOW_VAR_ALERT_EMAIL: str = "platform_alert_email"

# ─── Alert Thresholds (defaults) ──────────────────────────────────────────────
# Used when Airflow Variables are not set (graceful fallback).

DEFAULT_DEAD_LETTER_MAX_PER_HOUR: int = 100
DEFAULT_SILVER_MIN_ROWS_PER_HOUR: int = 50     # ~1 trade/min minimum expected
DEFAULT_INGESTOR_HEALTH_URL: str = "https://crypto-ingestor-<hash>-uc.a.run.app"

# ─── DAG Tags ─────────────────────────────────────────────────────────────────

TAG_TRANSFORMATION: str = "transformation"
TAG_MONITORING: str = "monitoring"
TAG_DBT: str = "dbt"
TAG_SILVER: str = "silver"
TAG_GOLD: str = "gold"
TAG_HEALTH: str = "health"
TAG_PLATFORM: str = "crypto-market-data"

# ─── Retry Policy ─────────────────────────────────────────────────────────────

TASK_RETRIES: int = 3
TASK_RETRY_DELAY_MINUTES: int = 1      # Base delay — doubles each retry (1→2→4 min)
TASK_EXECUTION_TIMEOUT_MINUTES: int = 30

# ─── SLA ──────────────────────────────────────────────────────────────────────

TRANSFORMATION_SLA_MINUTES: int = 30   # Full Silver+Gold pipeline must complete in 30 min
HEALTH_MONITOR_SLA_MINUTES: int = 10   # Health check cycle must complete in 10 min