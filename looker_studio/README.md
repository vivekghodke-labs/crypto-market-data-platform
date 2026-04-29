# Looker Studio Dashboard — BTC/USDT Analytics

## Overview

The Crypto Market Data Platform dashboard provides real-time and historical
analytics for BTC/USDT trade data ingested from the Binance WebSocket stream.
All data is sourced from the BigQuery Gold layer, produced by the dbt
transformation pipeline on a 15-minute schedule.

**Live Dashboard:** `[Add Looker Studio public URL after first publish]`

---

## Dashboard Architecture

```
Binance WebSocket
      ↓
Cloud Run Ingestor → Pub/Sub → Apache Beam → BigQuery Bronze/Silver
                                                      ↓
                                               dbt (every 15 min)
                                                      ↓
                                           BigQuery Gold Layer
                                                      ↓
                                         Looker Studio Dashboard
                                       (15-min data freshness cache)
```

---

## Pages

### Page 1 — Price Overview
**Data Sources:** `gold_daily_ohlcv`, `gold_moving_averages`

Key visualisations:
- Current price, 24h change, SMA-7, SMA-30 scorecards
- Trend signal and MA cross signal indicators
- Daily close price with SMA-7 and SMA-30 overlay (line chart)
- Daily OHLCV combo chart (high-low range bars + close line)
- Daily price change % bar chart (green/red by direction)

### Page 2 — Market Depth & VWAP
**Data Source:** `gold_price_stats_24h`

Key visualisations:
- True VWAP scorecard with price spread indicator
- 24h high, low, range scorecards
- Trade count and notional volume scorecards
- Maker/taker split bullet chart
- VWAP vs price midpoint grouped bar chart

### Page 3 — Volume Analysis
**Data Source:** `gold_trade_volume_hourly`

Key visualisations:
- Hourly notional volume + VWAP combo chart
- Volume heatmap by day-of-week × hour-of-day (UTC)
- Taker % trend line (market aggression indicator)

---

## Data Sources

| ID | Looker Studio Name | BigQuery Table | Refresh |
|---|---|---|---|
| DS-1 | `crypto_gold_daily_ohlcv` | `gold_analytics.gold_daily_ohlcv` | 15 min |
| DS-2 | `crypto_gold_price_stats_24h` | `gold_analytics.gold_price_stats_24h` | 15 min |
| DS-3 | `crypto_gold_trade_volume_hourly` | `gold_analytics.gold_trade_volume_hourly` | 15 min |
| DS-4 | `crypto_gold_moving_averages` | `gold_analytics.gold_moving_averages` | 15 min |

---

## Calculated Fields Summary

| ID | Name | Data Source | Purpose |
|---|---|---|---|
| CF-1 | Candle Colour | DS-1 | Green/red conditional colour for candlestick bars |
| CF-2 | Price Change Label | DS-1 | Directional ▲/▼ price change text |
| CF-3 | Volume USD M | DS-1 | Volume in millions for readable axis labels |
| CF-4 | Intraday Range % | DS-1 | Formatted volatility range |
| CF-5 | Current Price | DS-2 | `$` prefixed close price for scorecard |
| CF-6 | VWAP vs Close | DS-2 | Absolute VWAP spread |
| CF-7 | VWAP Spread Label | DS-2 | Formatted +/- VWAP spread |
| CF-8 | 24h Change Colour | DS-2 | Green/red for 24h change scorecard |
| CF-9 | Taker Dominance | DS-2 | Market microstructure classification |
| CF-10 | Hourly Volume USD M | DS-3 | Hourly volume in millions |
| CF-11 | Hour Label | DS-3 | "0:00"–"23:00" axis labels |
| CF-12 | Volume Intensity | DS-3 | Categorical volume bucket for heatmap |
| CF-13 | SMA 7D | DS-4 | Formatted SMA-7 with null guard |
| CF-14 | SMA 30D | DS-4 | Formatted SMA-30 with null guard |
| CF-15 | Trend Signal Colour | DS-4 | Green/red/amber scorecard background |
| CF-16 | Cross Signal Label | DS-4 | Golden/death cross emoji label |

Full calculated field formulas: see [`calculated_fields.md`](./calculated_fields.md)

---

## Setup Instructions

1. Ensure the dbt pipeline has run at least once successfully:
   ```bash
   make dbt-run
   make dbt-test
   ```

2. Verify Gold tables contain data:
   ```sql
   SELECT COUNT(*), MAX(trade_date)
   FROM `vg-ind-2026.gold_analytics.gold_daily_ohlcv`
   ```

3. Open [lookerstudio.google.com](https://lookerstudio.google.com) and follow
   [`dashboard_spec.md`](./dashboard_spec.md) for step-by-step chart configuration.

4. Connect each data source per [`data_sources.md`](./data_sources.md).

5. Add all calculated fields per [`calculated_fields.md`](./calculated_fields.md).

6. Set 15-minute data freshness on all data sources.

7. Publish and add the public URL to the `Live Dashboard` link above.

---

## SMA Warm-Up Period

The SMA-7 and SMA-30 lines require a minimum number of days of trading data
before they are displayed:

| Indicator | Days Required | Behaviour Before Ready |
|---|---|---|
| SMA-7 | 7 days | Shows "—" in scorecard, gap in line chart |
| SMA-30 | 30 days | Shows "—" in scorecard, gap in line chart |

This is by design — partial-window SMAs are mathematically misleading and
are excluded from the dashboard rather than displayed as inaccurate values.

---

## VWAP Methodology

The VWAP displayed on Page 2 uses the **true industry-standard formula**:

```
VWAP = SUM(price × quantity) / SUM(quantity)
```

Computed at **individual trade granularity** from `silver_curated.silver_deduped_trades`.
This matches the methodology used by Bloomberg Terminal, Refinitiv Eikon,
and ICE Data Services — not an approximation from OHLCV candle data.