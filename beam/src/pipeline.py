"""
Main Apache Beam pipeline entry point.

Composes all PTransforms from transforms.py into the full OHLCV pipeline.

Pipeline graph:
  ReadFromPubSub
      └── ParseAndValidate
              ├── valid   → AssignEventTimestamp
              │               └── WindowIntoFixedWindows(60s, lateness=30s)
              │                       └── ExtractKey
              │                               └── CombinePerKey(OHLCVCombineFn)
              │                                       └── FormatOHLCV
              │                                               └── WriteToBigQuery (Silver)
              └── dead_letter → WriteToBigQuery (Bronze dead_letter_log)

Usage:
    # Local DirectRunner (development)
    BEAM_RUNNER=direct python -m beam.src.pipeline

    # GCP DataflowRunner (demo burst)
    BEAM_RUNNER=dataflow python -m beam.src.pipeline
"""

import logging
import sys

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import (
    AfterWatermark,
    AfterProcessingTime,
    AccumulationMode,
)
from apache_beam.utils.timestamp import Duration

from .config import PipelineConfig, build_pipeline_options
from .schema import BQ_OHLCV_SCHEMA_STR
from .transforms import (
    ParseAndValidate,
    AssignEventTimestamp,
    ExtractKey,
    OHLCVCombineFn,
    FormatOHLCV,
    DEAD_LETTER_TAG,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# BigQuery dead-letter schema for pipeline-level parse failures
BQ_DEAD_LETTER_SCHEMA = "raw_message:STRING,pipeline_error:STRING,logged_at:TIMESTAMP"


def build_pipeline(config: PipelineConfig, options) -> beam.Pipeline:
    """
    Constructs and returns the configured Beam pipeline.
    Separated from run() to allow TestPipeline injection in unit tests.

    Args:
        config:  Validated PipelineConfig with all runtime settings.
        options: PipelineOptions from build_pipeline_options().

    Returns:
        A configured beam.Pipeline (not yet run).
    """
    pipeline = beam.Pipeline(options=options)

    # ── Source ────────────────────────────────────────────────────────────────
    raw_messages = pipeline | "ReadFromPubSub" >> ReadFromPubSub(
        subscription=config.pubsub_subscription,
        with_attributes=False,      # Raw bytes only — attributes not needed
        timestamp_attribute=None,   # We assign event timestamps manually
    )

    # ── Parse & Validate ──────────────────────────────────────────────────────
    parsed = raw_messages | "ParseAndValidate" >> ParseAndValidate()

    valid_trades = parsed["valid"]
    dead_letter_messages = parsed[DEAD_LETTER_TAG]

    # ── Event Timestamp Assignment ────────────────────────────────────────────
    # Must happen BEFORE windowing — Beam uses these timestamps to assign
    # elements to the correct fixed window.
    timestamped = valid_trades | "AssignEventTimestamp" >> beam.ParDo(
        AssignEventTimestamp()
    )

    # ── Fixed Window (60 seconds) ─────────────────────────────────────────────
    # Trigger strategy:
    #   AfterWatermark          → fire when Beam estimates all data has arrived
    #   early=AfterProcessingTime(30s) → speculative early firing every 30s
    #                                    for near-real-time Silver visibility
    #   late=AfterCount(1)      → fire immediately on any late element arrival
    #   ACCUMULATING            → accumulate panes (late data merged, not replaced)
    #
    # allowed_lateness=30s: trades arriving up to 30s after window close
    # are still included. Beyond 30s they are dropped (acceptable for OHLCV).
    windowed = timestamped | "WindowIntoFixedWindows" >> beam.WindowInto(
        FixedWindows(config.window_size_seconds),
        trigger=AfterWatermark(
            early=AfterProcessingTime(30),
        ),
        accumulation_mode=AccumulationMode.ACCUMULATING,
        allowed_lateness=Duration(seconds=config.allowed_lateness_seconds),
    )

    # ── Key by Symbol ─────────────────────────────────────────────────────────
    keyed = windowed | "ExtractKey" >> beam.ParDo(ExtractKey())

    # ── OHLCV Aggregation ─────────────────────────────────────────────────────
    aggregated = keyed | "AggregateOHLCV" >> beam.CombinePerKey(OHLCVCombineFn())

    # ── Format for BigQuery ───────────────────────────────────────────────────
    formatted = aggregated | "FormatOHLCV" >> beam.ParDo(FormatOHLCV())

    # ── Write Silver: OHLCV candles ───────────────────────────────────────────
    formatted | "WriteOHLCVToSilver" >> WriteToBigQuery(
        table=config.bq_silver_table,
        schema=BQ_OHLCV_SCHEMA_STR,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        create_disposition=BigQueryDisposition.CREATE_NEVER,  # Table managed by Terraform
        # Streaming inserts — required for near-real-time (<1s latency to BQ)
        method=WriteToBigQuery.Method.STREAMING_INSERTS,
    )

    # ── Write Dead Letter: pipeline parse failures ────────────────────────────
    # Formats raw string into a dict matching BQ_DEAD_LETTER_SCHEMA
    (
        dead_letter_messages
        | "FormatDeadLetter" >> beam.Map(
            lambda raw: {
                "raw_message": raw[:1024],  # Truncate to BQ STRING limit safety margin
                "pipeline_error": "parse_or_validation_failure",
                "logged_at": _now_iso(),
            }
        )
        | "WriteDeadLetterToBronze" >> WriteToBigQuery(
            table=f"{config.project_id}:bronze_raw.pipeline_dead_letter",
            schema=BQ_DEAD_LETTER_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        )
    )

    return pipeline


def run() -> None:
    """
    Builds and runs the OHLCV pipeline.
    Blocks until the pipeline completes (DirectRunner) or is submitted
    (DataflowRunner — returns after job submission, not completion).
    """
    config = PipelineConfig()
    options = build_pipeline_options(config)

    logger.info(
        "Starting OHLCV pipeline | runner=%s | window=%ds",
        config.runner,
        config.window_size_seconds,
    )

    pipeline = build_pipeline(config, options)

    result = pipeline.run()

    if config.runner == "direct":
        logger.info("Pipeline running (DirectRunner — streaming, Ctrl+C to stop)")
        result.wait_until_finish()
    else:
        logger.info(
            "Pipeline submitted to Dataflow. Monitor at: "
            "https://console.cloud.google.com/dataflow/jobs/%s",
            config.region,
        )


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    run()