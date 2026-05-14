"""
DuckDB / MotherDuck sink transforms for the Beam OHLCV pipeline.

Provides:
  WriteToDuckDB          — batched writer for OHLCV records → silver_curated.ohlcv_1min
  WriteDeadLetterToDuckDB — writer for parse failures → bronze_raw.pipeline_dead_letter
  WriteDeadLetterToDuckDB added; replaces LogDeadLetter log-only sink.
"""

import duckdb
from datetime import datetime, timezone
from typing import Iterable

import apache_beam as beam

from .logger import get_logger

logger = get_logger(__name__)


# ─── OHLCV Sink ───────────────────────────────────────────────────────────────
class DuckDBWriter(beam.DoFn):
    """Writes OHLCV records to DuckDB / MotherDuck."""

    def __init__(self, db_path: str, table: str, schema_sql: str):
        self.db_path = db_path
        self.table = table
        self.schema_sql = schema_sql
        self._conn = None

    def setup(self):
        self._conn = duckdb.connect(self.db_path)
        logger.info("DuckDB OHLCV writer initialized | table=%s", self.table)

    def process(self, batch: Iterable[dict]):
        records = list(batch)
        if not records:
            return

        columns = list(records[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        insert_sql = (
            f"INSERT INTO {self.table} ({', '.join(columns)}) "
            f"VALUES ({placeholders})"
        )
        rows = [tuple(r[col] for col in columns) for r in records]

        self._conn.executemany(insert_sql, rows)
        self._conn.commit()
        logger.info("Wrote %d OHLCV records to %s", len(records), self.table)

    def teardown(self):
        if self._conn:
            self._conn.close()


class WriteToDuckDB(beam.PTransform):
    """Batches and writes OHLCV records to DuckDB / MotherDuck silver table."""

    def __init__(self, db_path: str, table: str, schema_sql: str, batch_size: int = 100):
        self.db_path = db_path
        self.table = table
        self.schema_sql = schema_sql
        self.batch_size = batch_size

        # Ensure schema exists at pipeline startup (not per-worker).
        logger.info("Ensuring DuckDB schema exists | table=%s | path=%s", table, db_path)
        with duckdb.connect(db_path) as conn:
            conn.execute(schema_sql)

    def expand(self, pcoll):
        return (
            pcoll
            | "Batch OHLCV records" >> beam.BatchElements(
                min_batch_size=1, max_batch_size=self.batch_size
            )
            | "Write OHLCV to DuckDB" >> beam.ParDo(
                DuckDBWriter(self.db_path, self.table, self.schema_sql)
            )
        )


# ─── Dead-Letter Sink ─────────────────────────────────────────────────────────
class DeadLetterDuckDBWriter(beam.DoFn):
    """
    Writes dead-letter strings to bronze_raw.pipeline_dead_letter.

    Each element is a raw string (failed parse or validation).
    Columns written: raw_message (truncated to 1024), pipeline_error, logged_at.

    pipeline_error is set to 'beam_parse_or_validation_failure' — consistent
    with the error category used by the health monitor DAG query.
    """

    _ERROR_CATEGORY = "beam_parse_or_validation_failure"
    _MAX_MSG_LEN = 1024

    def __init__(self, db_path: str, schema_sql: str):
        self.db_path = db_path
        self.schema_sql = schema_sql
        self._conn = None

    def setup(self):
        self._conn = duckdb.connect(self.db_path)
        logger.info("DuckDB dead-letter writer initialized | path=%s", self.db_path)

    def process(self, element: str, *args, **kwargs):
        raw_message = str(element)[: self._MAX_MSG_LEN]
        logged_at = datetime.now(timezone.utc).isoformat()

        try:
            self._conn.execute(
                "INSERT INTO bronze_raw.pipeline_dead_letter "
                "(raw_message, pipeline_error, logged_at) VALUES (?, ?, ?)",
                [raw_message, self._ERROR_CATEGORY, logged_at],
            )
            self._conn.commit()
            logger.warning(
                "Dead-letter written to MotherDuck | error=%s | msg_preview=%s",
                self._ERROR_CATEGORY,
                raw_message[:80],
            )
        except Exception as exc:
            # Never crash the pipeline on a dead-letter write failure — log and continue.
            logger.error(
                "Failed to write dead-letter to DuckDB: %s | raw=%s",
                exc,
                raw_message[:80],
            )

    def teardown(self):
        if self._conn:
            self._conn.close()


class WriteDeadLetterToDuckDB(beam.PTransform):
    """
    Writes dead-letter messages to bronze_raw.pipeline_dead_letter
    in DuckDB / MotherDuck. Ensures schema exists at pipeline startup.
    """

    TABLE = "bronze_raw.pipeline_dead_letter"

    def __init__(self, db_path: str, schema_sql: str):
        self.db_path = db_path
        self.schema_sql = schema_sql

        logger.info(
            "Ensuring dead-letter schema exists | table=%s | path=%s",
            self.TABLE,
            db_path,
        )
        with duckdb.connect(db_path) as conn:
            conn.execute(schema_sql)

    def expand(self, pcoll):
        return (
            pcoll
            | "Write dead-letter to DuckDB" >> beam.ParDo(
                DeadLetterDuckDBWriter(self.db_path, self.schema_sql)
            )
        )