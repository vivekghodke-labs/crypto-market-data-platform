# Looker Studio — Data Source Connection Specifications

## Overview

This document defines the four BigQuery data sources required for the
Crypto Market Data Platform dashboard. Each data source maps directly to
a Gold layer BigQuery table produced by the dbt pipeline.

**GCP Project:** `vg-ind-2026`
**BigQuery Location:** `us-central1`
**Authentication:** Service account `sa-airflow@vg-ind-2026.iam.gserviceaccount.com`
(Looker Studio uses the viewer's Google account by default — ensure the
viewing account has `roles/bigquery.dataViewer` on `gold_analytics` dataset)

---

## DS-1: Daily OHLCV

| Property | Value |
|---|---|
| **Data Source Name** | `crypto_gold_daily_ohlcv` |
| **Connection Type** | BigQuery |
| **Project** | `vg-ind-2026` |
| **Dataset** | `gold_analytics` |
| **Table** | `gold_daily_ohlcv` |
| **Data Freshness** | 15 minutes (matches dbt pipeline schedule) |
| **Date Field** | `trade_date` |

### Field Configuration

| Field Name | Type | Aggregation | Description |
|---|---|---|---|
| `trade_date` | Date | — | Daily candle date. Use as time dimension. |
| `symbol` | Text | — | Trading pair. Default filter: BTCUSDT. |
| `open` | Number | None | Daily open price (NUMERIC → auto DECIMAL). |
| `high` | Number | Max | Daily high price. |
| `low` | Number | Min | Daily low price. |
| `close` | Number | None | Daily close price. |
| `volume` | Number | Sum | Notional USD volume. |
| `trade_count` | Number | Sum | Number of individual trades. |
| `candle_count` | Number | Sum | Number of 1-minute candles. |
| `price_change` | Number | None | Close − open (absolute). |
| `price_change_pct` | Number | None | Daily % change. |
| `intraday_range` | Number | None | High − low. |
| `candle_direction` | Text | — | `bullish` or `bearish`. |
| `dbt_updated_at` | Date & Time | — | Pipeline run timestamp. |

---

## DS-2: 24-Hour Price Statistics

| Property | Value |
|---|---|
| **Data Source Name** | `crypto_gold_price_stats_24h` |
| **Connection Type** | BigQuery |
| **Project** | `vg-ind-2026` |
| **Dataset** | `gold_analytics` |
| **Table** | `gold_price_stats_24h` |
| **Data Freshness** | 15 minutes |
| **Date Field** | `stats_as_of` |

### Field Configuration

| Field Name | Type | Aggregation | Description |
|---|---|---|---|
| `symbol` | Text | — | Trading pair. |
| `stats_as_of` | Date & Time | Max | Anchor timestamp for 24h window. |
| `open_24h` | Number | None | 24h window open price. |
| `high_24h` | Number | Max | 24h high. |
| `low_24h` | Number | Min | 24h low. |
| `close_24h` | Number | None | Most recent close price. Use as **Current Price**. |
| `vwap` | Number | None | True VWAP: SUM(P×Q)/SUM(Q). |
| `total_notional` | Number | Sum | Total USD notional volume. |
| `total_quantity` | Number | Sum | Total BTC quantity traded. |
| `price_change_24h` | Number | None | Absolute price change over 24h. |
| `price_change_pct_24h` | Number | None | Percentage price change over 24h. |
| `range_24h` | Number | None | High − low for the 24h window. |
| `vwap_vs_midpoint` | Number | None | VWAP − (high+low)/2. |
| `trade_count_24h` | Number | Sum | Total trades in 24h window. |
| `maker_trade_count` | Number | Sum | Maker-side trade count. |
| `taker_trade_count` | Number | Sum | Taker-side trade count. |
| `taker_pct` | Number | None | Taker percentage of total volume. |
| `market_direction_24h` | Text | — | `bullish` or `bearish`. |

---

## DS-3: Hourly Trade Volume

| Property | Value |
|---|---|
| **Data Source Name** | `crypto_gold_trade_volume_hourly` |
| **Connection Type** | BigQuery |
| **Project** | `vg-ind-2026` |
| **Dataset** | `gold_analytics` |
| **Table** | `gold_trade_volume_hourly` |
| **Data Freshness** | 15 minutes |
| **Date Field** | `trade_hour` |

### Field Configuration

| Field Name | Type | Aggregation | Description |
|---|---|---|---|
| `trade_hour` | Date & Time | — | Hour bucket (UTC). Partition key. |
| `trade_date` | Date | — | Calendar date. |
| `hour_of_day` | Number | — | 0–23. Use for heatmap dimension. |
| `day_of_week` | Text | — | Monday–Sunday. |
| `symbol` | Text | — | Trading pair. |
| `notional_volume` | Number | Sum | USD volume for the hour. |
| `btc_volume` | Number | Sum | BTC volume for the hour. |
| `vwap_hourly` | Number | None | Hourly true VWAP. |
| `high` | Number | Max | Hourly price high. |
| `low` | Number | Min | Hourly price low. |
| `trade_count` | Number | Sum | Trade count for the hour. |
| `taker_pct` | Number | None | Taker percentage. |
| `avg_trade_size_usd` | Number | None | Average USD trade size. |

---

## DS-4: Moving Averages

| Property | Value |
|---|---|
| **Data Source Name** | `crypto_gold_moving_averages` |
| **Connection Type** | BigQuery |
| **Project** | `vg-ind-2026` |
| **Dataset** | `gold_analytics` |
| **Table** | `gold_moving_averages` |
| **Data Freshness** | 15 minutes |
| **Date Field** | `trade_date` |

### Field Configuration

| Field Name | Type | Aggregation | Description |
|---|---|---|---|
| `trade_date` | Date | — | Calendar date. Partition key. |
| `symbol` | Text | — | Trading pair. |
| `close` | Number | None | Daily close price. |
| `sma_7d` | Number | None | 7-day SMA. NULL for first 6 days. |
| `sma_30d` | Number | None | 30-day SMA. NULL for first 29 days. |
| `sma_7d_valid` | Boolean | — | Filter: show only valid SMA-7 values. |
| `sma_30d_valid` | Boolean | — | Filter: show only valid SMA-30 values. |
| `price_vs_sma_7d_pct` | Number | None | Price momentum vs SMA-7 (%). |
| `price_vs_sma_30d_pct` | Number | None | Price momentum vs SMA-30 (%). |
| `trend_signal` | Text | — | `above_both` / `below_both` / `mixed` / `insufficient_history`. |
| `cross_signal` | Text | — | `golden_cross_zone` / `death_cross_zone` / `at_crossover`. |
| `days_of_history` | Number | Max | Cumulative days of data available. |