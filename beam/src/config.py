"""
Apache Beam pipeline options and runtime configuration.

Two runner profiles are supported, selected via the BEAM_RUNNER env var:

  BEAM_RUNNER=direct    → DirectRunner (local, M2 Mac via OrbStack)
  BEAM_RUNNER=dataflow  → DataflowRunner (GCP burst demo, ~$0.20–0.50 for 10 min)

All configuration is sourced from environment variables — no hardcoded values.
This follows the 12-factor app methodology and keeps secrets out of code.

Usage:
    options = build_pipeline_options()
    with beam.Pipeline(options=options) as p:
        ...
"""

import os
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    GoogleCloudOptions,
    WorkerOptions,
    SetupOptions,
)

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

WINDOW_SIZE_SECONDS: int = 60
ALLOWED_LATENESS_SECONDS: int = 30

# ─── Environment ──────────────────────────────────────────────────────────────

def _get_required_env(key: str) -> str:
    """Reads a required environment variable. Raises clearly on missing."""
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Check your .env file or Cloud Run environment configuration."
        )
    return value


def _get_env(key: str, default: str) -> str:
    return os.getenv(key, default)


# ─── Config Dataclass ─────────────────────────────────────────────────────────

class PipelineConfig:
    """
    Centralises all pipeline runtime configuration.
    Validated at startup — fail-fast on missing required values.
    """

    def __init__(self) -> None:
        self.runner: str = _get_env("BEAM_RUNNER", "direct").lower()
        self.project_id: str = _get_required_env("GCP_PROJECT_ID")
        self.region: str = _get_env("GCP_REGION", "us-central1")

        # Pub/Sub
        self.pubsub_subscription: str = (
            f"projects/{self.project_id}/subscriptions/"
            f"{_get_env('PUBSUB_SUBSCRIPTION', 'btc-raw-trades-beam-sub')}"
        )

        # BigQuery targets
        self.bq_silver_table: str = (
            f"{self.project_id}:"
            f"{_get_env('BQ_SILVER_DATASET', 'silver_curated')}."
            f"{_get_env('BQ_SILVER_TABLE', 'ohlcv_1min')}"
        )

        # Dataflow-specific (only required when BEAM_RUNNER=dataflow)
        self.gcs_temp_location: str = _get_env(
            "GCS_TEMP_LOCATION",
            f"gs://{self.project_id}-dataflow-staging/temp",
        )
        self.gcs_staging_location: str = _get_env(
            "GCS_STAGING_LOCATION",
            f"gs://{self.project_id}-dataflow-staging/staging",
        )
        self.dataflow_service_account: str = _get_env(
            "DATAFLOW_SERVICE_ACCOUNT",
            f"sa-dataflow@{self.project_id}.iam.gserviceaccount.com",
        )

        # Window configuration
        self.window_size_seconds: int = WINDOW_SIZE_SECONDS
        self.allowed_lateness_seconds: int = ALLOWED_LATENESS_SECONDS

        self._log_config()

    def _log_config(self) -> None:
        logger.info(
            "Pipeline configuration loaded | runner=%s | project=%s | "
            "subscription=%s | silver_table=%s | window=%ds | lateness=%ds",
            self.runner,
            self.project_id,
            self.pubsub_subscription,
            self.bq_silver_table,
            self.window_size_seconds,
            self.allowed_lateness_seconds,
        )


# ─── Pipeline Options Builder ─────────────────────────────────────────────────

def build_pipeline_options(config: PipelineConfig) -> PipelineOptions:
    """
    Constructs Apache Beam PipelineOptions for the selected runner.

    DirectRunner:  No GCP credentials required. All processing in-process.
    DataflowRunner: Requires GCP project, GCS bucket, and sa-dataflow SA.

    Args:
        config: Validated PipelineConfig instance.

    Returns:
        Configured PipelineOptions ready to pass to beam.Pipeline().
    """
    if config.runner == "direct":
        return _build_direct_options(config)
    elif config.runner == "dataflow":
        return _build_dataflow_options(config)
    else:
        raise ValueError(
            f"Unknown BEAM_RUNNER='{config.runner}'. "
            f"Valid values: 'direct', 'dataflow'."
        )


def _build_direct_options(config: PipelineConfig) -> PipelineOptions:
    """DirectRunner options — local execution, no GCP auth required."""
    options = PipelineOptions(flags=[])

    standard = options.view_as(StandardOptions)
    standard.runner = "DirectRunner"
    # Enable streaming mode — required for Pub/Sub source + windowing
    standard.streaming = True

    logger.info("Pipeline configured for DirectRunner (local)")
    return options


def _build_dataflow_options(config: PipelineConfig) -> PipelineOptions:
    """
    DataflowRunner options — GCP managed execution for demo burst runs.

    Worker configuration is intentionally minimal for cost control:
      machine_type: n1-standard-1  → cheapest available worker
      max_num_workers: 1           → single worker, sufficient for demo
      disk_size_gb: 30             → minimum allowed by Dataflow
    """
    options = PipelineOptions(flags=[])

    standard = options.view_as(StandardOptions)
    standard.runner = "DataflowRunner"
    standard.streaming = True

    gcp = options.view_as(GoogleCloudOptions)
    gcp.project = config.project_id
    gcp.region = config.region
    gcp.job_name = "btc-ohlcv-pipeline"
    gcp.temp_location = config.gcs_temp_location
    gcp.staging_location = config.gcs_staging_location
    gcp.service_account_email = config.dataflow_service_account

    worker = options.view_as(WorkerOptions)
    worker.machine_type = "n1-standard-1"
    worker.max_num_workers = 1
    worker.disk_size_gb = 30

    setup = options.view_as(SetupOptions)
    setup.save_main_session = True

    logger.info(
        "Pipeline configured for DataflowRunner | job=btc-ohlcv-pipeline | "
        "region=%s | max_workers=1 | machine=n1-standard-1",
        config.region,
    )
    return options