###############################################################################
# bigquery.tf
# BigQuery datasets implementing the Medallion architecture.
#
#   Bronze  → raw, unmodified trade events (schema-on-read)
#   Silver  → cleaned, deduplicated, 1-minute OHLCV aggregations (Sprint 2/3)
#   Gold    → analytics-ready materialised views for Looker Studio (Sprint 3)
#
# Sprint 1 creates the datasets and the Bronze raw_trades table.
# Silver/Gold tables are created by dbt in Sprint 3 — only the datasets
# are provisioned here as placeholders with correct IAM.
#
# Free tier: BigQuery provides 10 GB storage + 1 TB queries/month free.
###############################################################################

# ─── Bronze Dataset ───────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "bronze" {
  dataset_id                  = var.bigquery_dataset_bronze
  friendly_name               = "Bronze — Raw Trade Events"
  description                 = "Raw BTC/USDT trade events as received from Binance WebSocket. Schema-on-read. Managed by Terraform."
  location                    = var.region
  project                     = var.project_id
  delete_contents_on_destroy  = false

  labels = {
    layer       = "bronze"
    environment = "portfolio"
    managed_by  = "terraform"
  }

  depends_on = [google_project_service.enabled_apis]
}

# Bronze table: raw_trades
# This is the landing table written by the Beam pipeline in Sprint 2.
# Defined here in Terraform so Beam has a target schema to write to.
resource "google_bigquery_table" "bronze_raw_trades" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "raw_trades"
  project             = var.project_id
  deletion_protection = false # Allow drop during development

  description = "Raw BTC/USDT trade events. One row per trade. Written by Beam pipeline."

  # Partition by ingestion date — dramatically reduces query cost in Gold layer.
  time_partitioning {
    type  = "DAY"
    field = null # null = partition by ingestion time (_PARTITIONTIME)
  }

  # Cluster by trade execution time for efficient time-range queries.
  clustering = ["trade_time_ms"]

  schema = jsonencode([
    {
      name        = "event_type"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Binance event type. Always 'trade' for this stream."
    },
    {
      name        = "event_time_ms"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Event publish timestamp in milliseconds (Unix epoch)."
    },
    {
      name        = "symbol"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Trading pair symbol. Always 'BTCUSDT' for this stream."
    },
    {
      name        = "trade_id"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Unique trade ID assigned by Binance. Used for deduplication."
    },
    {
      name        = "price"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Trade execution price in USDT. Stored as NUMERIC for precision."
    },
    {
      name        = "quantity"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Trade quantity in BTC. Stored as NUMERIC for precision."
    },
    {
      name        = "trade_time_ms"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Trade execution timestamp in milliseconds (Unix epoch)."
    },
    {
      name        = "is_market_maker"
      type        = "BOOLEAN"
      mode        = "REQUIRED"
      description = "True if the buyer is the market maker."
    },
    {
      name        = "ingested_at"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Timestamp when the record was written to BigQuery by the Beam pipeline."
    }
  ])

  labels = {
    layer  = "bronze"
    source = "binance-ws"
  }
}

# ─── Silver Dataset ───────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "silver" {
  dataset_id                  = var.bigquery_dataset_silver
  friendly_name               = "Silver — Curated & Aggregated"
  description                 = "Deduplicated, cleaned trade data and 1-minute OHLCV aggregations. Written by dbt (Sprint 3)."
  location                    = var.region
  project                     = var.project_id
  delete_contents_on_destroy  = false

  labels = {
    layer       = "silver"
    environment = "portfolio"
    managed_by  = "terraform"
  }

  depends_on = [google_project_service.enabled_apis]
}

# ─── Gold Dataset ─────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "gold" {
  dataset_id                  = var.bigquery_dataset_gold
  friendly_name               = "Gold — Analytics Ready"
  description                 = "Materialised analytics views for Looker Studio dashboards. Written by dbt (Sprint 3)."
  location                    = var.region
  project                     = var.project_id
  delete_contents_on_destroy  = false

  labels = {
    layer       = "gold"
    environment = "portfolio"
    managed_by  = "terraform"
  }

  depends_on = [google_project_service.enabled_apis]
}

# ─── Silver Table: ohlcv_1min ─────────────────────────────────────────────────
# Target table for the Beam OHLCV pipeline (Sprint 2).
# Beam uses CREATE_NEVER — this table must exist before the pipeline runs.
# Partitioned by window_start DAY for cost-efficient Looker Studio queries.

resource "google_bigquery_table" "silver_ohlcv_1min" {
  dataset_id          = google_bigquery_dataset.silver.dataset_id
  table_id            = "ohlcv_1min"
  project             = var.project_id
  deletion_protection = false

  description = "1-minute OHLCV candles for BTC/USDT. Written by Apache Beam pipeline. Partitioned by window_start."

  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }

  clustering = ["symbol"]

  schema = jsonencode([
    {
      name        = "window_start"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Fixed window start time (UTC). Partition key."
    },
    {
      name        = "window_end"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Fixed window end time (UTC). Always window_start + 60s."
    },
    {
      name        = "symbol"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Trading pair. Always BTCUSDT at portfolio scope."
    },
    {
      name        = "open"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Price of the first trade in the window (by trade_time_ms)."
    },
    {
      name        = "high"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Maximum trade price within the window."
    },
    {
      name        = "low"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Minimum trade price within the window."
    },
    {
      name        = "close"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Price of the last trade in the window (by trade_time_ms)."
    },
    {
      name        = "volume"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Sum of (price × quantity) for all trades in the window."
    },
    {
      name        = "trade_count"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Number of individual trades aggregated into this candle."
    },
    {
      name        = "ingested_at"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Timestamp when this record was written by the Beam pipeline."
    }
  ])

  labels = {
    layer    = "silver"
    pipeline = "beam-ohlcv"
    sprint   = "2"
  }
}

# ─── Bronze Table: pipeline_dead_letter ───────────────────────────────────────
# Captures Beam-level parse failures (distinct from ingestor dead-letter topic).
# Created with CREATE_IF_NEEDED by the pipeline — defined here for visibility
# and to enable monitoring queries from the start.

resource "google_bigquery_table" "bronze_pipeline_dead_letter" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "pipeline_dead_letter"
  project             = var.project_id
  deletion_protection = false

  description = "Pipeline-level parse/validation failures from the Beam OHLCV pipeline."

  time_partitioning {
    type  = "DAY"
    field = null
  }

  schema = jsonencode([
    {
      name        = "raw_message"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Original raw message string (truncated to 1024 chars)."
    },
    {
      name        = "pipeline_error"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Error category from the Beam pipeline."
    },
    {
      name        = "logged_at"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Timestamp when this failure was logged by the pipeline."
    }
  ])

  labels = {
    layer   = "bronze"
    purpose = "dead-letter"
    sprint  = "2"
  }
}

# ─── BigQuery IAM ─────────────────────────────────────────────────────────────

# Beam pipeline SA (Sprint 2) will need dataEditor on Bronze.
# Pre-granted here to ingestor SA as a placeholder — Sprint 2 will
# introduce a dedicated sa-beam service account.
###############################################################################
# Gold Layer Tables
# Managed by Terraform — dbt uses CREATE OR REPLACE TABLE on full refresh.
# Datasets already provisioned in Sprint 1. Tables defined here for:
#   1. Schema documentation in IaC
#   2. IAM binding targets
#   3. Looker Studio data source stability
###############################################################################

resource "google_bigquery_table" "gold_moving_averages" {
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "gold_moving_averages"
  project             = var.project_id
  deletion_protection = false
  description         = "7-day and 30-day SMA of BTC/USDT daily close. Written by dbt. One row per (symbol, trade_date)."

  time_partitioning {
    type  = "DAY"
    field = "trade_date"
  }

  clustering = ["symbol"]

  labels = {
    layer    = "gold"
    pipeline = "dbt"
    sprint   = "5"
  }
}

resource "google_bigquery_table" "gold_daily_ohlcv" {
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "gold_daily_ohlcv"
  project             = var.project_id
  deletion_protection = false
  description         = "Daily OHLCV candlestick data. Written by dbt. One row per (symbol, trade_date)."

  time_partitioning {
    type  = "DAY"
    field = "trade_date"
  }

  clustering = ["symbol"]

  labels = {
    layer    = "gold"
    pipeline = "dbt"
    sprint   = "3"
  }
}

resource "google_bigquery_table" "gold_price_stats_24h" {
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "gold_price_stats_24h"
  project             = var.project_id
  deletion_protection = false
  description         = "24-hour rolling price statistics including true VWAP. Written by dbt. One row per symbol."

  clustering = ["symbol"]

  labels = {
    layer    = "gold"
    pipeline = "dbt"
    sprint   = "3"
  }
}

resource "google_bigquery_table" "gold_trade_volume_hourly" {
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "gold_trade_volume_hourly"
  project             = var.project_id
  deletion_protection = false
  description         = "Hourly trade volume and activity metrics. Written by dbt. One row per (symbol, trade_hour)."

  time_partitioning {
    type  = "HOUR"
    field = "trade_hour"
  }

  clustering = ["symbol"]

  labels = {
    layer    = "gold"
    pipeline = "dbt"
    sprint   = "3"
  }
}

# dbt test failure storage dataset
resource "google_bigquery_dataset" "dbt_test_failures" {
  dataset_id                 = "dbt_test_failures"
  friendly_name              = "dbt Test Failures"
  description                = "Stores failing rows from dbt schema and custom data tests. Used for data quality triage."
  location                   = var.region
  project                    = var.project_id
  delete_contents_on_destroy = false

  labels = {
    layer      = "ops"
    managed_by = "terraform"
  }

  depends_on = [google_project_service.enabled_apis]
}

# ─── dbt Service Account ──────────────────────────────────────────────────────

resource "google_service_account" "sa_dbt" {
  account_id   = "sa-dbt"
  display_name = "Crypto Platform — dbt Transformation Runner"
  description  = "Runtime identity for dbt Core model execution. Read Silver, write Gold."
  project      = var.project_id
}

# Read from Bronze (source freshness checks + silver_deduped_trades reads Bronze)
resource "google_bigquery_dataset_iam_member" "dbt_bronze_viewer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.bronze.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.sa_dbt.email}"
}

# Read from Silver (source for Gold models)
resource "google_bigquery_dataset_iam_member" "dbt_silver_viewer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.sa_dbt.email}"
}

# Write to Gold (dbt model output)
resource "google_bigquery_dataset_iam_member" "dbt_gold_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa_dbt.email}"
}

# Write to dbt_test_failures dataset
resource "google_bigquery_dataset_iam_member" "dbt_test_failures_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.dbt_test_failures.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa_dbt.email}"
}

# BigQuery job runner — required to execute any query
resource "google_project_iam_member" "dbt_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.sa_dbt.email}"
}

# Allow CI/CD SA to act as dbt SA (for Airflow job submission in Sprint 4)
resource "google_service_account_iam_member" "cicd_act_as_dbt" {
  service_account_id = google_service_account.sa_dbt.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.sa_cicd.email}"
}

# ─── BigQuery IAM (existing) ──────────────────────────────────────────────────

resource "google_bigquery_dataset_iam_member" "ingestor_bronze_editor" {
  dataset_id = google_bigquery_dataset.bronze.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.sa_ingestor.email}"
  project    = var.project_id
}
