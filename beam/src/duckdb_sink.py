import duckdb
from typing import Iterable

import apache_beam as beam
from apache_beam.io import iobase

from .logger import get_logger

logger = get_logger(__name__)


class DuckDBWriter(beam.DoFn):
    """Writes OHLCV records to DuckDB."""

    def __init__(self, db_path: str, table: str, schema_sql: str):
        self.db_path = db_path
        self.table = table
        self.schema_sql = schema_sql
        self._conn = None

    def setup(self):
        """Initialize connection (per worker)."""
        # Open connection but REMOVE the table creation from here
        self._conn = duckdb.connect(self.db_path)
        logger.info(f"DuckDB writer initialized | table={self.table}")

    def process(self, batch: Iterable[dict]):
        """Batch write OHLCV records."""
        records = list(batch)
        if not records:
            return

        # Prepare INSERT statement
        columns = records[0].keys()
        placeholders = ", ".join(["?" for _ in columns])
        insert_sql = f"INSERT INTO {self.table} ({', '.join(columns)}) VALUES ({placeholders})"

        # Extract values
        rows = [tuple(r[col] for col in columns) for r in records]

        # Batch insert
        self._conn.executemany(insert_sql, rows)
        self._conn.commit()
        logger.info(f"Wrote {len(records)} records to {self.table}")

    def teardown(self):
        """Close connection on shutdown."""
        if self._conn:
            self._conn.close()


class WriteToDuckDB(beam.PTransform):
    """Batches and writes records to DuckDB table."""

    def __init__(self, db_path: str, table: str, schema_sql: str, batch_size: int = 100):
        self.db_path = db_path
        self.table = table
        self.schema_sql = schema_sql
        self.batch_size = batch_size

        # EXECUTED ON PIPELINE STARTUP: Create schema immediately
        logger.info(f"Ensuring DuckDB schema exists for {self.table} at {self.db_path}")
        with duckdb.connect(self.db_path) as conn:
            conn.execute(self.schema_sql)

    def expand(self, pcoll):
        return (
            pcoll
            | "Batch records" >> beam.BatchElements(min_batch_size=1, max_batch_size=self.batch_size)
            | "Write to DuckDB" >> beam.ParDo(DuckDBWriter(self.db_path, self.table, self.schema_sql))
        )