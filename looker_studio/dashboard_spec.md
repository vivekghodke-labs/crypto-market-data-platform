# Looker Studio — Dashboard Configuration Specification

## Report Setup

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com)
2. Click **Create → Report**
3. **Report Name:** `Crypto Market Data Platform — BTC/USDT Analytics`
4. **Theme:** Dark (Edit → Theme and Layout → Dark)
5. **Canvas Size:** 1366 × 768 (widescreen standard)

---

## Report-Level Date Range Filter

Apply this filter to ALL pages so every chart responds to the same date range.

1. **Page → Manage Pages → Report Settings**
2. Add **Date Range Control**:
   - Default period: **Last 7 days**
   - Data source: DS-1 (`gold_daily_ohlcv`)
   - Date field: `trade_date`
3. Add **Symbol Filter** (Drop-down list):
   - Data source: DS-1
   - Dimension: `symbol`
   - Default value: `BTCUSDT`
   - Apply to all pages: ✅

---

## Page 1: Price Overview

**Page Title:** `Price Overview`
**Data Sources:** DS-1 (Daily OHLCV), DS-4 (Moving Averages)

---

### Section A: Header Scorecards (Row 1 — top of page)

#### Chart 1-A: Current Price Scorecard
| Property | Value |
|---|---|
| Chart type | Scorecard |
| Data source | DS-2 (`gold_price_stats_24h`) |
| Metric | `close_24h` |
| Label | `BTC/USDT Current Price` |
| Comparison metric | `open_24h` |
| Comparison label | `24h Open` |
| Number format | Currency USD, 2 decimal places |
| Font size (value) | 32pt Bold |
| Background colour | #1e1e2e |

#### Chart 1-B: 24h Change Scorecard
| Property | Value |
|---|---|
| Chart type | Scorecard |
| Data source | DS-2 |
| Metric | `price_change_pct_24h` |
| Label | `24h Change %` |
| Conditional colour | CF-8 (`24h Change Colour`) |
| Number format | Percent, 2 decimal places |
| Font size (value) | 28pt Bold |

#### Chart 1-C: SMA 7D Scorecard
| Property | Value |
|---|---|
| Chart type | Scorecard |
| Data source | DS-4 (`gold_moving_averages`) |
| Metric | `sma_7d` |
| Aggregation | Max (most recent value) |
| Label | `7-Day SMA` |
| Number format | Currency USD, 2 decimal places |
| Comparison metric | `close` |
| Note | Shows "—" automatically when NULL (insufficient history) |

#### Chart 1-D: SMA 30D Scorecard
| Property | Value |
|---|---|
| Chart type | Scorecard |
| Data source | DS-4 |
| Metric | `sma_30d` |
| Aggregation | Max |
| Label | `30-Day SMA` |
| Number format | Currency USD, 2 decimal places |

#### Chart 1-E: Trend Signal Scorecard
| Property | Value |
|---|---|
| Chart type | Scorecard |
| Data source | DS-4 |
| Metric | `trend_signal` |
| Aggregation | — (text dimension, show latest) |
| Label | `Trend Signal` |
| Background colour formula | CF-15 (`Trend Signal Colour`) |

#### Chart 1-F: Cross Signal Scorecard
| Property | Value |
|---|---|
| Chart type | Scorecard |
| Data source | DS-4 |
| Metric | `CF-16` (`Cross Signal Label`) |
| Label | `MA Cross Signal` |

---

### Section B: Price Chart with SMA Overlay (Main chart)

#### Chart 1-G: Daily Close + SMA Line Chart
| Property | Value |
|---|---|
| Chart type | Line Chart |
| Data source | DS-4 (`gold_moving_averages`) |
| Date range dimension | `trade_date` |
| Dimension | `trade_date` |
| Metrics (3 series) | `close`, `sma_7d`, `sma_30d` |
| Series colours | close = #4fc3f7 (blue), sma_7d = #ffb74d (amber), sma_30d = #f06292 (pink) |
| Series line style | close = solid 2px, sma_7d = dashed 1.5px, sma_30d = dashed 1.5px |
| Axis | Left Y: Price (USD), X: Date |
| Grid lines | Horizontal only, #333333 |
| Legend | Bottom, labels: "Close", "SMA 7D", "SMA 30D" |
| Null handling | Connect gaps (NULL SMA values during warm-up are skipped) |
| Tooltip | Show all 3 series values on hover |
| Filter | `sma_7d IS NOT NULL OR sma_30d IS NOT NULL` — hides rows before any SMA available |
| Size | Full width, 280px height |

---

### Section C: Daily OHLCV Candlestick Representation

**Note:** Looker Studio does not have a native candlestick chart type.
The industry-standard workaround is a **Combo Chart (Bar + Line)**:

#### Chart 1-H: Daily OHLCV Combo Chart
| Property | Value |
|---|---|
| Chart type | Combo Chart |
| Data source | DS-1 (`gold_daily_ohlcv`) |
| Date dimension | `trade_date` |
| Bar metric | `intraday_range` — represents the high-low wick |
| Line metric | `close` — close price overlay |
| Stacked base | `low` (invisible series, sets bar base) |
| Bar colour | CF-1 (`Candle Colour`) — green/red per direction |
| Line colour | #ffffff (white) |
| Y-axis | Left: Price range (USD) |
| Tooltip | show `open`, `high`, `low`, `close`, `candle_direction` |
| Size | Half width left, 240px height |

#### Chart 1-I: Price Change % Bar Chart
| Property | Value |
|---|---|
| Chart type | Bar Chart |
| Data source | DS-1 |
| Dimension | `trade_date` |
| Metric | `price_change_pct` |
| Bar colour | CF-1 (`Candle Colour`) |
| Reference line | Y=0 (zero line) colour #ffffff, style dashed |
| Y-axis label | `Daily Change (%)` |
| Size | Half width right, 240px height |

---

## Page 2: Market Depth & VWAP

**Page Title:** `Market Depth & VWAP`
**Data Source:** DS-2 (`gold_price_stats_24h`)

---

### Section A: Key Metrics Row

#### Chart 2-A: VWAP Scorecard
| Property | Value |
|---|---|
| Chart type | Scorecard |
| Metric | `vwap` |
| Label | `True VWAP (24h)` |
| Subtitle | `SUM(P×Q) / SUM(Q)` |
| Number format | Currency USD, 2 decimal places |
| Comparison metric | `close_24h` |
| Comparison label | `vs Current Price` |

#### Chart 2-B: VWAP Spread Scorecard
| Property | Value |
|---|---|
| Chart type | Scorecard |
| Metric | CF-7 (`VWAP Spread Label`) |
| Label | `Price vs VWAP` |
| Conditional colour | Green if `close_24h >= vwap`, Red otherwise |

#### Chart 2-C: 24h High Scorecard
| Property | Value |
|---|---|
| Chart type | Scorecard |
| Metric | `high_24h` |
| Label | `24h High` |
| Number format | Currency USD, 2 decimal places |

#### Chart 2-D: 24h Low Scorecard
| Property | Value |
|---|---|
| Chart type | Scorecard |
| Metric | `low_24h` |
| Label | `24h Low` |
| Number format | Currency USD, 2 decimal places |

#### Chart 2-E: 24h Range Scorecard
| Property | Value |
|---|---|
| Chart type | Scorecard |
| Metric | `range_24h` |
| Label | `24h Range (USD)` |
| Number format | Currency USD, 2 decimal places |

#### Chart 2-F: Trade Count Scorecard
| Property | Value |
|---|---|
| Chart type | Scorecard |
| Metric | `trade_count_24h` |
| Label | `Trades (24h)` |
| Number format | Decimal, 0 places, compact notation |

---

### Section B: Maker/Taker Split

#### Chart 2-G: Maker vs Taker Donut Chart
| Property | Value |
|---|---|
| Chart type | Pie Chart (set to Donut style) |
| Data source | DS-2 |
| Dimension | Custom blend: create two rows "Maker" / "Taker" |
| Approach | Use **Blended Data** with two separate scorecards OR a **Pivot Table** |
| Alternative | Use a **Bullet Chart** with `taker_pct` metric and 50% reference line |
| Colours | Taker: #ef5350 (red), Maker: #26a69a (green) |
| Label | `Maker/Taker Split (24h)` |
| Show legend | ✅ Bottom |

**Implementation note:** Looker Studio cannot directly pivot `maker_trade_count`
and `taker_trade_count` into a single donut without blended data. The simplest
production approach is two stacked scorecards (Maker % and Taker %) with a
horizontal bullet/progress bar showing the split ratio.

#### Chart 2-H: Taker % Bullet Chart (alternative to donut)
| Property | Value |
|---|---|
| Chart type | Bullet Chart |
| Metric | `taker_pct` |
| Target value | 50 (midpoint) |
| Range 1 | 0–40 (Maker Dominated) colour #26a69a |
| Range 2 | 40–60 (Balanced) colour #ffb74d |
| Range 3 | 60–100 (Taker Dominated) colour #ef5350 |
| Label | `Taker % (24h)` |

---

### Section C: VWAP vs Midpoint Bar Chart

#### Chart 2-I: VWAP vs Price Midpoint
| Property | Value |
|---|---|
| Chart type | Bar Chart |
| Data source | DS-2 |
| Dimension | `symbol` |
| Metrics | `vwap`, midpoint (calculated: `(high_24h + low_24h) / 2`) |
| Bar style | Grouped |
| Colours | VWAP: #4fc3f7, Midpoint: #b0bec5 |
| Label | `VWAP vs Price Midpoint` |
| Note | VWAP above midpoint = net buying pressure |

---

## Page 3: Volume Analysis

**Page Title:** `Volume Analysis`
**Data Source:** DS-3 (`gold_trade_volume_hourly`)

---

### Section A: Hourly Volume Bar Chart

#### Chart 3-A: Hourly Notional Volume
| Property | Value |
|---|---|
| Chart type | Bar Chart |
| Data source | DS-3 |
| Date range dimension | `trade_hour` |
| Dimension | `trade_hour` |
| Metric | CF-10 (`Hourly Volume USD M`) |
| Y-axis label | `Volume (USD Millions)` |
| Bar colour | #4fc3f7 |
| Sort | `trade_hour` ascending |
| Size | Full width, 240px height |

---

### Section B: VWAP Hourly Overlay

#### Chart 3-B: Hourly VWAP Line Chart
| Property | Value |
|---|---|
| Chart type | Line Chart |
| Data source | DS-3 |
| Dimension | `trade_hour` |
| Metric | `vwap_hourly` |
| Line colour | #ffb74d (amber) |
| Line style | Solid 2px |
| Y-axis | Right axis (secondary) |
| Label | `Hourly VWAP` |
| Tooltip | Show `vwap_hourly`, `notional_volume`, `trade_count` |
| Size | Full width, 240px height (overlay with Chart 3-A as Combo Chart) |

**Implementation:** Combine Chart 3-A and 3-B into a single **Combo Chart**:
- Bars: `Hourly Volume USD M`
- Line: `vwap_hourly`
- Left Y: Volume (USD M), Right Y: VWAP Price

---

### Section C: Trade Volume Heatmap by Hour of Day

#### Chart 3-C: Hour-of-Day Volume Heatmap
| Property | Value |
|---|---|
| Chart type | Pivot Table with Heatmap |
| Data source | DS-3 |
| Row dimension | `day_of_week` (sort: Mon→Sun) |
| Column dimension | CF-11 (`Hour Label`) → `hour_of_day` (0–23) |
| Metric | `notional_volume` |
| Heatmap | Enabled — diverging colour scale |
| Heatmap colours | Low: #1a1a2e (dark blue), High: #ef5350 (red) |
| Cell size | Compact |
| Number format | Compact USD (e.g., "$2.3M") |
| Label | `Volume Heatmap: Day × Hour (UTC)` |
| Size | Full width, 200px height |

---

### Section D: Taker Percentage Trend

#### Chart 3-D: Taker % Over Time
| Property | Value |
|---|---|
| Chart type | Line Chart |
| Data source | DS-3 |
| Dimension | `trade_hour` |
| Metric | `taker_pct` |
| Line colour | #ef5350 (red) |
| Reference line | Y=50, style dashed, colour #ffffff, label "Equal Split" |
| Y-axis | 0–100%, label "Taker %" |
| Shading | Area fill below line, opacity 20% |
| Label | `Taker % Trend (Aggression Indicator)` |
| Size | Half width right, 200px height |

---

## Report-Level Settings

### Data Freshness
1. **Resource → Manage Added Data Sources → [Each Source] → Edit**
2. Set **Data freshness**: `15 minutes`
3. Apply to all 4 data sources.

### Report Filters
Already applied at report level (see Report Setup above):
- Date range: default Last 7 days
- Symbol: default BTCUSDT

### Sharing
1. **File → Share → Manage access**
2. Set to **Anyone with the link can view**
3. Copy the report URL and add to `README.md` under the Dashboard link placeholder.

---

## Rebuild Checklist

Use this checklist to reproduce the report from scratch:

- [ ] Create report in Looker Studio
- [ ] Apply dark theme
- [ ] Connect DS-1 (gold_daily_ohlcv)
- [ ] Connect DS-2 (gold_price_stats_24h)
- [ ] Connect DS-3 (gold_trade_volume_hourly)
- [ ] Connect DS-4 (gold_moving_averages)
- [ ] Add all calculated fields (CF-1 through CF-16)
- [ ] Add report-level date range filter
- [ ] Add report-level symbol filter
- [ ] Build Page 1 — Price Overview (9 charts)
- [ ] Build Page 2 — Market Depth & VWAP (9 charts)
- [ ] Build Page 3 — Volume Analysis (4 charts)
- [ ] Set 15-minute data freshness on all data sources
- [ ] Set report sharing to public view
- [ ] Copy public URL to README.md