"""
Apache Beam pipeline configuration — DuckDB + Kafka mode.

Changes from GCP version:
- Removed PUBSUB_SUBSCRIPTION, GCS_* variables
- Added KAFKA_*, DUCKDB_PATH
- Removed DataflowRunner support (local-only for now)
"""

import os
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

logger = logging.getLogger(__name__)

WINDOW_SIZE_SECONDS: int = 5
ALLOWED_LATENESS_SECONDS: int = 30


def _get_required_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return value


def _get_env(key: str, default: str) -> str:
    return os.getenv(key, default)


class PipelineConfig:
    """Pipeline runtime configuration."""

    def __init__(self) -> None:
        # Kafka (Redpanda Cloud)
        self.kafka_bootstrap_servers: str = _get_required_env("KAFKA_BOOTSTRAP_SERVERS")
        self.kafka_sasl_username: str = _get_required_env("KAFKA_SASL_USERNAME")
        self.kafka_sasl_password: str = _get_required_env("KAFKA_SASL_PASSWORD")
        self.kafka_topic: str = _get_env("KAFKA_TOPIC_RAW_TRADES", "btc-raw-trades")
        self.kafka_consumer_group: str = _get_env("KAFKA_CONSUMER_GROUP", "beam-ohlcv-pipeline")

        # DuckDB / MotherDuck — BUGFIX: duckdb_path_raw was referenced before assignment.
        # Resolve local path first, then override if MOTHERDUCK_TOKEN is present.
        duckdb_path_raw: str = _get_env("DUCKDB_PATH", "/data/duckdb/crypto_platform.db")

        # If MOTHERDUCK_TOKEN is set, force MotherDuck path
        motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
        if motherduck_token:
            self.duckdb_path = f"md:crypto_platform?motherduck_token={motherduck_token}"
            logger.info("Using MotherDuck connection")
        else:
            self.duckdb_path = duckdb_path_raw
            logger.info("Using local DuckDB: %s", self.duckdb_path)

        # Window config
        self.window_size_seconds: int = WINDOW_SIZE_SECONDS
        self.allowed_lateness_seconds: int = ALLOWED_LATENESS_SECONDS

        self._log_config()

    def _log_config(self) -> None:
        logger.info(
            "Pipeline configuration loaded | kafka_bootstrap=%s | duckdb_path=%s | window=%ds",
            self.kafka_bootstrap_servers,
            self.duckdb_path,
            self.window_size_seconds,
        )


def build_pipeline_options(config: PipelineConfig) -> PipelineOptions:
    """Builds FlinkRunner pipeline options."""
    options = PipelineOptions(flags=[])
    standard = options.view_as(StandardOptions)
    standard.runner = "FlinkRunner"
    standard.streaming = True

    logger.info("Pipeline configured for FlinkRunner (local)")
    return options