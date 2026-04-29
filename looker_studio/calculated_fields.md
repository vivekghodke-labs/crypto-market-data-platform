# Looker Studio — Calculated Fields

## Overview

Calculated fields extend the raw BigQuery table columns with
display-oriented transformations. All financial logic remains in dbt —
these fields handle formatting, colour logic, and UI presentation only.

Create each field in the data source editor:
**Resource → Manage Added Data Sources → [Select Source] → Add Field**

---

## DS-1 Calculated Fields (gold_daily_ohlcv)

### CF-1: Candle Colour
**Field Name:** `Candle Colour`
**Data Source:** DS-1
**Formula:**
```
IF(candle_direction = "bullish", "#26a69a", "#ef5350")
```
**Usage:** Conditional colour for bar charts — green for bullish, red for bearish.
Matches TradingView / Bloomberg standard candlestick colour convention.

---

### CF-2: Price Change Label
**Field Name:** `Price Change Label`
**Data Source:** DS-1
**Formula:**
```
CONCAT(
  IF(price_change_pct > 0, "▲ ", "▼ "),
  ROUND(price_change_pct, 2),
  "%"
)
```
**Usage:** Scorecard label showing directional price change with arrow indicator.

---

### CF-3: Volume (USD Millions)
**Field Name:** `Volume USD M`
**Data Source:** DS-1
**Formula:**
```
ROUND(volume / 1000000, 2)
```
**Usage:** Y-axis labels on volume charts — prevents scientific notation for
large numbers. Display unit: "M USD".

---

### CF-4: Daily Range Pct Label
**Field Name:** `Intraday Range %`
**Data Source:** DS-1
**Formula:**
```
CONCAT(ROUND(intraday_range_pct, 2), "%")
```
**Usage:** Tooltip and scorecard display of intraday volatility range.

---

## DS-2 Calculated Fields (gold_price_stats_24h)

### CF-5: Current Price Formatted
**Field Name:** `Current Price`
**Data Source:** DS-2
**Formula:**
```
CONCAT("$", ROUND(close_24h, 2))
```
**Usage:** Primary price scorecard on Page 1 header. Prefix with $ symbol.

---

### CF-6: VWAP Spread
**Field Name:** `VWAP vs Close`
**Data Source:** DS-2
**Formula:**
```
ROUND(close_24h - vwap, 2)
```
**Usage:** Scorecard showing whether current price is trading above or below
VWAP. Positive = price premium over VWAP (bullish pressure).

---

### CF-7: VWAP Spread Formatted
**Field Name:** `VWAP Spread Label`
**Data Source:** DS-2
**Formula:**
```
CONCAT(
  IF((close_24h - vwap) >= 0, "+$", "-$"),
  ABS(ROUND(close_24h - vwap, 2))
)
```
**Usage:** Formatted VWAP spread for scorecard subtitle.

---

### CF-8: 24h Change Direction Colour
**Field Name:** `24h Change Colour`
**Data Source:** DS-2
**Formula:**
```
IF(price_change_pct_24h >= 0, "#26a69a", "#ef5350")
```
**Usage:** Background colour for the 24h price change scorecard.

---

### CF-9: Taker Dominance Label
**Field Name:** `Taker Dominance`
**Data Source:** DS-2
**Formula:**
```
IF(
  taker_pct >= 60, "Taker Dominated",
  IF(taker_pct <= 40, "Maker Dominated", "Balanced")
)
```
**Usage:** Label in the maker/taker donut chart legend.

---

## DS-3 Calculated Fields (gold_trade_volume_hourly)

### CF-10: Volume (USD Millions) Hourly
**Field Name:** `Hourly Volume USD M`
**Data Source:** DS-3
**Formula:**
```
ROUND(notional_volume / 1000000, 3)
```
**Usage:** Y-axis label for hourly volume bar chart.

---

### CF-11: Hour Label
**Field Name:** `Hour Label`
**Data Source:** DS-3
**Formula:**
```
CONCAT(hour_of_day, ":00")
```
**Usage:** X-axis label on heatmap (0:00, 1:00 ... 23:00).

---

### CF-12: Volume Intensity
**Field Name:** `Volume Intensity`
**Data Source:** DS-3
**Formula:**
```
IF(
  notional_volume >= 5000000, "Very High",
  IF(notional_volume >= 2000000, "High",
  IF(notional_volume >= 500000, "Medium", "Low"))
)
```
**Usage:** Heatmap colour bucket for trade volume by hour-of-day.
Thresholds in USD: Very High ≥ $5M, High ≥ $2M, Medium ≥ $500K.

---

## DS-4 Calculated Fields (gold_moving_averages)

### CF-13: SMA 7d Formatted
**Field Name:** `SMA 7D`
**Data Source:** DS-4
**Formula:**
```
IF(sma_7d_valid, CONCAT("$", ROUND(sma_7d, 2)), "—")
```
**Usage:** Scorecard displaying current 7-day SMA. Shows "—" during warm-up.

---

### CF-14: SMA 30d Formatted
**Field Name:** `SMA 30D`
**Data Source:** DS-4
**Formula:**
```
IF(sma_30d_valid, CONCAT("$", ROUND(sma_30d, 2)), "—")
```
**Usage:** Scorecard displaying current 30-day SMA.

---

### CF-15: Trend Signal Colour
**Field Name:** `Trend Signal Colour`
**Data Source:** DS-4
**Formula:**
```
CASE
  WHEN trend_signal = "above_both" THEN "#26a69a"
  WHEN trend_signal = "below_both" THEN "#ef5350"
  WHEN trend_signal = "mixed"      THEN "#ff9800"
  ELSE                                  "#9e9e9e"
END
```
**Usage:** Conditional background colour for the Trend Signal scorecard.
Green = bullish, Red = bearish, Amber = mixed, Grey = insufficient history.

---

### CF-16: Cross Signal Label
**Field Name:** `Cross Signal Label`
**Data Source:** DS-4
**Formula:**
```
CASE
  WHEN cross_signal = "golden_cross_zone" THEN "🟡 Golden Cross Zone"
  WHEN cross_signal = "death_cross_zone"  THEN "💀 Death Cross Zone"
  WHEN cross_signal = "at_crossover"      THEN "⚡ Crossover"
  ELSE                                         "— Insufficient History"
END
```
**Usage:** Descriptive label for the cross signal scorecard on Page 1.