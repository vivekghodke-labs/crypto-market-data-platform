# Crypto Market Data Platform — dbt Project

## Overview

This dbt project implements the **Medallion Architecture** transformation layer
for the Crypto Market Data Platform. It transforms raw BTC/USDT trade events
ingested from the Binance WebSocket stream into analytics-ready Gold layer tables
consumed by Looker Studio.

## Data Lineage

```
[Binance WebSocket]
      ↓
[Cloud Run Ingestor] → [Pub/Sub: btc-raw-trades]
      ↓
[Apache Beam Pipeline]
      ↓
bronze_raw.raw_trades          ← Individual trades (may contain duplicates)
bronze_raw.pipeline_dead_letter← Beam parse failures
      ↓ dbt (silver_deduped_trades)
silver_curated.silver_deduped_trades ← Deduplicated trades (unique on trade_id)

[Apache Beam Pipeline]
      ↓
silver_curated.ohlcv_1min      ← 1-min OHLCV candles (may have duplicate panes)
      ↓ dbt (silver_ohlcv_validated)
silver_curated.silver_ohlcv_validated ← Validated, deduplicated candles (VIEW)
      ↓
gold_analytics.gold_daily_ohlcv       ← Daily OHLCV rollup
gold_analytics.gold_price_stats_24h   ← True VWAP + 24h rolling stats
gold_analytics.gold_trade_volume_hourly ← Hourly volume breakdown
```

## VWAP Methodology

The `gold_price_stats_24h` model implements **true VWAP** per the industry standard:

```sql
VWAP = SUM(price × quantity) / SUM(quantity)
```

- Computed at **individual trade granularity** from `silver_deduped_trades`
- No approximation from OHLCV candles
- Matches methodology used by Bloomberg Terminal, Refinitiv Eikon, ICE Data Services

## Running the Project

```bash
# Install dependencies
cd dbt/crypto_platform
pip install dbt-bigquery
dbt deps

# Authenticate (local development)
gcloud auth application-default login

# Compile (validates SQL — no BigQuery connection needed)
dbt compile --profiles-dir .

# Run all models
dbt run --profiles-dir .

# Run tests
dbt test --profiles-dir .

# Run specific layer
dbt run --select silver --profiles-dir .
dbt run --select gold --profiles-dir .

# Source freshness check
dbt source freshness --profiles-dir .

# Generate and serve documentation
dbt docs generate --profiles-dir .
dbt docs serve
```

## Model Reference

| Model | Layer | Materialisation | Grain | Partition |
|---|---|---|---|---|
| `silver_deduped_trades` | Silver | Incremental (merge) | 1 row / trade | trade_date (DAY) |
| `silver_ohlcv_validated` | Silver | View | 1 row / (symbol, window) | — |
| `gold_daily_ohlcv` | Gold | Table | 1 row / (symbol, date) | trade_date (DAY) |
| `gold_price_stats_24h` | Gold | Table | 1 row / symbol | — |
| `gold_trade_volume_hourly` | Gold | Table | 1 row / (symbol, hour) | trade_hour (HOUR) |

## Data Quality Tests

| Test | Target | Type |
|---|---|---|
| `not_null`, `unique` on `trade_id` | silver_deduped_trades | Schema |
| `accepted_values` on `symbol` | All models | Schema |
| `not_null` on all key columns | All models | Schema |
| `assert_ohlcv_high_gte_low` | silver_ohlcv_validated | Custom SQL |
| `assert_ohlcv_open_close_within_range` | silver_ohlcv_validated | Custom SQL |
| `assert_no_future_timestamps` | silver_ohlcv_validated | Custom SQL |