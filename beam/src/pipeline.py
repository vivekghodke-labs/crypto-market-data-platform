"""
Main Apache Beam pipeline — Kafka → MotherDuck (DuckDB).

- Replaced LogDeadLetter (log-only) with WriteDeadLetterToDuckDB sink.
- DUCKDB_DEAD_LETTER_SCHEMA imported and passed to dead-letter sink.
- WriteDeadLetterToDuckDB imported from duckdb_sink.
"""

import logging
import sys
import apache_beam as beam

from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode
from .config import PipelineConfig, build_pipeline_options
from .schema import DUCKDB_SILVER_SCHEMA, DUCKDB_DEAD_LETTER_SCHEMA
from .transforms import (
    ParseAndValidate,
    AssignEventTimestamp,
    ExtractKey,
    OHLCVCombineFn,
    FormatOHLCV,
    DEAD_LETTER_TAG,
)
from .kafka_source import ReadFromKafka
from .duckdb_sink import WriteToDuckDB, WriteDeadLetterToDuckDB, WriteRawTradesToDuckDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def build_pipeline(config: PipelineConfig, options) -> beam.Pipeline:
    """Constructs the Beam pipeline graph."""
    pipeline = beam.Pipeline(options=options)

    # ── Source: Kafka (Redpanda Cloud) ────────────────────────────────────────
    raw_messages = pipeline | "ReadFromKafka" >> ReadFromKafka(
        bootstrap_servers=config.kafka_bootstrap_servers,
        topic=config.kafka_topic,
        consumer_group=config.kafka_consumer_group,
        sasl_username=config.kafka_sasl_username,
        sasl_password=config.kafka_sasl_password,
    )

    # ── Parse & Validate ──────────────────────────────────────────────────────
    parsed = raw_messages | "ParseAndValidate" >> ParseAndValidate()
    valid_trades = parsed["valid"]
    dead_letter_messages = parsed[DEAD_LETTER_TAG]

    # ── Bronze: raw trades → MotherDuck ───────────────────────────────────────
    # Wired BEFORE windowing — every valid tick persisted to bronze_raw.raw_trades
    valid_trades | "WriteRawTrades" >> WriteRawTradesToDuckDB(
        db_path=config.duckdb_path,
    )

    # ── Dead-letter sink → bronze_raw.pipeline_dead_letter (MotherDuck) ───────
    dead_letter_messages | "WriteDeadLetter" >> WriteDeadLetterToDuckDB(
        db_path=config.duckdb_path,
        schema_sql=DUCKDB_DEAD_LETTER_SCHEMA,
    )

    # ── Event Timestamp ───────────────────────────────────────────────────────
    timestamped = valid_trades | "AssignEventTimestamp" >> beam.ParDo(
        AssignEventTimestamp()
    )

    # ── Fixed Window (60s) ────────────────────────────────────────────────────
    windowed = timestamped | "WindowIntoFixedWindows" >> beam.WindowInto(
        FixedWindows(config.window_size_seconds),
        trigger=AfterWatermark(
            # Optional: Emit early partial candles before the window closes
            early=AfterProcessingTime(5),
            # Emit updated candles if late trades arrive
            late=AfterProcessingTime(5)
        ),
        accumulation_mode=AccumulationMode.ACCUMULATING,
        allowed_lateness=config.allowed_lateness_seconds # e.g., 60 seconds
    )

    # ── Key by Symbol ─────────────────────────────────────────────────────────
    keyed = windowed | "ExtractKey" >> beam.ParDo(ExtractKey())

    # ── OHLCV Aggregation ─────────────────────────────────────────────────────
    aggregated = keyed | "AggregateOHLCV" >> beam.CombinePerKey(OHLCVCombineFn())

    # ── Format for DuckDB ─────────────────────────────────────────────────────
    formatted = aggregated | "FormatOHLCV" >> beam.ParDo(FormatOHLCV())

    # ── Write OHLCV → silver_curated.ohlcv_1min (MotherDuck) ─────────────────
    formatted | "WriteOHLCVToDuckDB" >> WriteToDuckDB(
        db_path=config.duckdb_path,
        table="silver_curated.ohlcv_1min",
        schema_sql=DUCKDB_SILVER_SCHEMA,
        batch_size=1,
    )

    return pipeline


def run() -> None:
    """Builds and runs the pipeline."""
    config = PipelineConfig()
    options = build_pipeline_options(config)

    logger.info(
        "Starting OHLCV pipeline | runner=DirectRunner | window=%ds",
        config.window_size_seconds,
    )

    pipeline = build_pipeline(config, options)
    result = pipeline.run()

    logger.info("Pipeline running (DirectRunner — streaming, Ctrl+C to stop)")
    result.wait_until_finish()


if __name__ == "__main__":
    run()