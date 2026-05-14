"""
Data contracts for the Beam OHLCV pipeline.

Defines:
  TradeRecord         — Parsed, validated trade event from Pub/Sub message.
  OHLCVRecord         — Aggregated 1-minute OHLCV candle written to BigQuery Silver.
  BQ_OHLCV_SCHEMA     — BigQuery table schema for silver_curated.ohlcv_1min.
  DUCKDB_BRONZE_SCHEMA         — DDL for bronze_raw.raw_trades (MotherDuck).
  DUCKDB_DEAD_LETTER_SCHEMA    — DDL for bronze_raw.pipeline_dead_letter (MotherDuck).
  DUCKDB_SILVER_SCHEMA         — DDL for silver_curated.ohlcv_1min (MotherDuck).

Design note:
  We use plain dataclasses (not Pydantic) for Beam element types.
  Beam serialises elements between transforms using pickle. Pydantic models
  carry heavy metaclass machinery that can cause issues with Beam's
  distributed serialisation. Dataclasses are lightweight and pickle cleanly.

  Pydantic is used only at the boundary (ParseAndValidate transform) to
  validate the raw Pub/Sub JSON — after validation, data is converted to
  TradeRecord dataclass for all downstream Beam processing.
"""

from dataclasses import dataclass, field
from decimal import Decimal


# ─── Beam Element Types ───────────────────────────────────────────────────────

@dataclass
class TradeRecord:
    """
    A single validated BTC/USDT trade event.
    This is the element type flowing through the Beam pipeline after
    ParseAndValidate. All downstream PTransforms operate on TradeRecord.
    """
    trade_id: int
    symbol: str
    price: Decimal
    quantity: Decimal
    trade_time_ms: int        # Trade execution time — used as Beam event timestamp
    event_time_ms: int        # Binance event publish time


@dataclass
class OHLCVAccumulator:
    """
    Mutable accumulator used by OHLCVCombineFn during the CombinePerKey step.
    Holds running state for one symbol within one fixed window.
    """
    open: Decimal = Decimal("0")
    high: Decimal = Decimal("0")
    low: Decimal = Decimal("0")
    close: Decimal = Decimal("0")
    volume: Decimal = Decimal("0")
    trade_count: int = 0
    first_trade_time_ms: int = 0
    last_trade_time_ms: int = 0
    is_empty: bool = True


@dataclass
class OHLCVRecord:
    """
    A completed 1-minute OHLCV candle written to BigQuery Silver layer.
    One record per (symbol, window) pair.
    """
    window_start: str
    window_end: str
    symbol: str
    open: str
    high: str
    low: str
    close: str
    volume: str
    trade_count: int
    ingested_at: str


# ─── BigQuery Schema ──────────────────────────────────────────────────────────

BQ_OHLCV_SCHEMA = {
    "fields": [
        {"name": "window_start",  "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end",    "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "symbol",        "type": "STRING",    "mode": "REQUIRED"},
        {"name": "open",          "type": "NUMERIC",   "mode": "REQUIRED"},
        {"name": "high",          "type": "NUMERIC",   "mode": "REQUIRED"},
        {"name": "low",           "type": "NUMERIC",   "mode": "REQUIRED"},
        {"name": "close",         "type": "NUMERIC",   "mode": "REQUIRED"},
        {"name": "volume",        "type": "NUMERIC",   "mode": "REQUIRED"},
        {"name": "trade_count",   "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "ingested_at",   "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

BQ_OHLCV_SCHEMA_STR = (
    "window_start:TIMESTAMP,"
    "window_end:TIMESTAMP,"
    "symbol:STRING,"
    "open:NUMERIC,"
    "high:NUMERIC,"
    "low:NUMERIC,"
    "close:NUMERIC,"
    "volume:NUMERIC,"
    "trade_count:INTEGER,"
    "ingested_at:TIMESTAMP"
)

# ─── DuckDB / MotherDuck Schemas ──────────────────────────────────────────────

DUCKDB_BRONZE_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS bronze_raw;

CREATE TABLE IF NOT EXISTS bronze_raw.raw_trades (
    event_type VARCHAR NOT NULL,
    event_time_ms BIGINT NOT NULL,
    symbol VARCHAR NOT NULL,
    trade_id BIGINT NOT NULL,
    price DECIMAL(18, 8) NOT NULL,
    quantity DECIMAL(18, 8) NOT NULL,
    trade_time_ms BIGINT NOT NULL,
    is_market_maker BOOLEAN NOT NULL,
    ingested_at TIMESTAMP NOT NULL
);
"""

DUCKDB_DEAD_LETTER_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS bronze_raw;

CREATE TABLE IF NOT EXISTS bronze_raw.pipeline_dead_letter (
    raw_message VARCHAR,
    pipeline_error VARCHAR,
    logged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

DUCKDB_SILVER_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS silver_curated;

CREATE TABLE IF NOT EXISTS silver_curated.ohlcv_1min (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    symbol VARCHAR NOT NULL,
    open DECIMAL(18, 8) NOT NULL,
    high DECIMAL(18, 8) NOT NULL,
    low DECIMAL(18, 8) NOT NULL,
    close DECIMAL(18, 8) NOT NULL,
    volume DECIMAL(18, 8) NOT NULL,
    trade_count INTEGER NOT NULL,
    ingested_at TIMESTAMP NOT NULL
);
"""