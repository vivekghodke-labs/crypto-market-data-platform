"""
Main Apache Beam pipeline — Kafka → DuckDB.

Changes from GCP version:
- Replaced ReadFromPubSub with custom KafkaSource
- Replaced WriteToBigQuery with WriteToDuckDB
- Removed dead-letter BigQuery write (log to file instead)
"""

import logging
import sys

import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount
from apache_beam.utils.timestamp import Duration

from .config import PipelineConfig, build_pipeline_options
from .schema import DUCKDB_BRONZE_SCHEMA, DUCKDB_SILVER_SCHEMA
from .transforms import (
    ParseAndValidate,
    AssignEventTimestamp,
    ExtractKey,
    OHLCVCombineFn,
    FormatOHLCV,
    DEAD_LETTER_TAG,
)
from .kafka_source import ReadFromKafka
from .duckdb_sink import WriteToDuckDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)


def build_pipeline(config: PipelineConfig, options) -> beam.Pipeline:
    """Constructs the Beam pipeline."""
    pipeline = beam.Pipeline(options=options)

    # Source: Kafka
    raw_messages = pipeline | "ReadFromKafka" >> ReadFromKafka(
        bootstrap_servers=config.kafka_bootstrap_servers,
        topic=config.kafka_topic,
        consumer_group=config.kafka_consumer_group,
        sasl_username=config.kafka_sasl_username,
        sasl_password=config.kafka_sasl_password,
    )

    raw_messages | "DebugPrintKafka" >> beam.Map(
        lambda msg: logger.info(f"DEBUG - KAFKA MESSAGE ARRIVED: {msg[:100]}")
    )

    # Parse & Validate
    parsed = raw_messages | "ParseAndValidate" >> ParseAndValidate()
    valid_trades = parsed["valid"]
    dead_letter_messages = parsed[DEAD_LETTER_TAG]

    # Dead-letter: Write to local log file (DuckDB dead-letter table later)
    dead_letter_messages | "LogDeadLetter" >> beam.Map(
        lambda msg: logger.warning(f"Dead letter: {msg[:200]}")
    )

    # Event Timestamp
    timestamped = valid_trades | "AssignEventTimestamp" >> beam.ParDo(AssignEventTimestamp())

    # Fixed Window (60s)
    windowed = timestamped | "WindowIntoFixedWindows" >> beam.WindowInto(
        FixedWindows(config.window_size_seconds)
    )

    # Key by Symbol
    keyed = windowed | "ExtractKey" >> beam.ParDo(ExtractKey())

    # OHLCV Aggregation
    aggregated = keyed | "AggregateOHLCV" >> beam.CombinePerKey(OHLCVCombineFn())

    # Format for DuckDB
    formatted = aggregated | "FormatOHLCV" >> beam.ParDo(FormatOHLCV())

    # ADD THIS DEBUG LINE:
    formatted | "DebugCandle" >> beam.Map(
        lambda c: logger.info(f"CANDLE GENERATED: {c['window_start']} to {c['window_end']} | Vol: {c['volume']}")
    )

    # Write to DuckDB Silver
    formatted | "WriteOHLCVToDuckDB" >> WriteToDuckDB(
        db_path=config.duckdb_path,
        table="silver.ohlcv_1min",
        schema_sql=DUCKDB_SILVER_SCHEMA,
        batch_size=1
    )

    return pipeline


def run() -> None:
    """Builds and runs the pipeline."""
    config = PipelineConfig()
    options = build_pipeline_options(config)

    logger.info("Starting OHLCV pipeline | runner=DirectRunner | window=%ds", config.window_size_seconds)

    pipeline = build_pipeline(config, options)
    result = pipeline.run()

    logger.info("Pipeline running (DirectRunner — streaming, Ctrl+C to stop)")
    result.wait_until_finish()


if __name__ == "__main__":
    run()