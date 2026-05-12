-- MotherDuck Schema Initialization
-- Run this once after creating your MotherDuck database

-- ══════════════════════════════════════════════════════════════════════════
-- BRONZE LAYER
-- ══════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS bronze_raw;

CREATE TABLE IF NOT EXISTS bronze_raw.raw_trades (
    event_type VARCHAR NOT NULL,
    event_time_ms BIGINT NOT NULL,
    symbol VARCHAR NOT NULL,
    trade_id BIGINT NOT NULL PRIMARY KEY,
    price DECIMAL(18, 8) NOT NULL,
    quantity DECIMAL(18, 8) NOT NULL,
    trade_time_ms BIGINT NOT NULL,
    is_market_maker BOOLEAN NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    -- Partition hint for MotherDuck query optimizer
    trade_date DATE GENERATED ALWAYS AS (DATE_TRUNC('day', epoch_ms(trade_time_ms)))
);

CREATE INDEX idx_raw_trades_time ON bronze_raw.raw_trades(trade_time_ms);
CREATE INDEX idx_raw_trades_date ON bronze_raw.raw_trades(trade_date);

-- Sequence for Dead letter table auto-incrementing ID
CREATE SEQUENCE IF NOT EXISTS bronze_raw.seq_dead_letter_id;

-- Dead letter table
CREATE TABLE IF NOT EXISTS bronze_raw.pipeline_dead_letter (
    id BIGINT DEFAULT nextval('bronze_raw.seq_dead_letter_id') PRIMARY KEY,
    raw_message VARCHAR(1024),
    pipeline_error VARCHAR,
    logged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ══════════════════════════════════════════════════════════════════════════
-- SILVER LAYER
-- ══════════════════════════════════════════════════════════════════════════

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
    ingested_at TIMESTAMP NOT NULL,
    candle_date DATE GENERATED ALWAYS AS (DATE_TRUNC('day', window_start)),
    PRIMARY KEY (symbol, window_start)
);

CREATE INDEX idx_ohlcv_date ON silver_curated.ohlcv_1min(candle_date);

-- ══════════════════════════════════════════════════════════════════════════
-- GOLD LAYER (dbt will create these)
-- ══════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS gold_analytics;

-- Gold tables will be created by dbt materializations