###############################################################################
# Materialisation: table (full refresh, partitioned by trade_date)
# Target: gold_analytics.gold_daily_ohlcv
#
# Purpose:
#   Rolls up 1-minute OHLCV candles into daily candlesticks.
#   One row per (symbol, trade_date).
#
# OHLCV rollup logic (standard exchange convention):
#   Open  → open of the first 1-minute candle of the day (by window_start)
#   High  → MAX(high) across all 1-minute candles of the day
#   Low   → MIN(low) across all 1-minute candles of the day
#   Close → close of the last 1-minute candle of the day (by window_start)
#   Volume→ SUM(volume) — notional USD volume for the day
#
# Primary consumer: Looker Studio daily candlestick chart.
###############################################################################

{{
    config(
        materialized = 'table',
        partition_by = {
            'field': 'trade_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by = ['symbol']
    )
}}

with

minute_candles as (

    select
        candle_date         as trade_date,
        symbol,
        window_start,
        open,
        high,
        low,
        close,
        volume,
        trade_count

    from {{ ref('silver_ohlcv_validated') }}

),

-- Identify the first and last 1-minute candle of each day per symbol.
-- Used to extract the daily open and close prices.
day_boundaries as (

    select
        trade_date,
        symbol,
        min(window_start) as first_candle_start,
        max(window_start) as last_candle_start

    from minute_candles
    group by 1, 2

),

-- Extract open (from first candle) and close (from last candle).
open_prices as (

    select
        mc.trade_date,
        mc.symbol,
        mc.open as daily_open

    from minute_candles mc
    inner join day_boundaries db
        on  mc.trade_date    = db.trade_date
        and mc.symbol        = db.symbol
        and mc.window_start  = db.first_candle_start

),

close_prices as (

    select
        mc.trade_date,
        mc.symbol,
        mc.close as daily_close

    from minute_candles mc
    inner join day_boundaries db
        on  mc.trade_date    = db.trade_date
        and mc.symbol        = db.symbol
        and mc.window_start  = db.last_candle_start

),

-- Aggregate all intraday metrics
daily_aggregates as (

    select
        trade_date,
        symbol,
        max(high)                   as daily_high,
        min(low)                    as daily_low,
        sum(volume)                 as daily_volume,
        sum(trade_count)            as daily_trade_count,
        count(*)                    as candle_count          -- number of 1-min candles

    from minute_candles
    group by 1, 2

),

final as (

    select
        da.trade_date,
        da.symbol,
        op.daily_open                                   as open,
        da.daily_high                                   as high,
        da.daily_low                                    as low,
        cp.daily_close                                  as close,
        da.daily_volume                                 as volume,
        da.daily_trade_count                            as trade_count,
        da.candle_count,

        -- Price change metrics
        cp.daily_close - op.daily_open                  as price_change,
        {{ safe_divide(
            '(cp.daily_close - op.daily_open)',
            'op.daily_open'
        ) }} * 100                                      as price_change_pct,

        -- Intraday range
        da.daily_high - da.daily_low                    as intraday_range,
        {{ safe_divide(
            '(da.daily_high - da.daily_low)',
            'op.daily_open'
        ) }} * 100                                      as intraday_range_pct,

        -- Candle classification
        case
            when cp.daily_close >= op.daily_open then 'bullish'
            else 'bearish'
        end                                             as candle_direction,

        current_timestamp()                             as dbt_updated_at

    from daily_aggregates    da
    inner join open_prices   op on da.trade_date = op.trade_date and da.symbol = op.symbol
    inner join close_prices  cp on da.trade_date = cp.trade_date and da.symbol = cp.symbol

)

select * from final
order by trade_date desc, symbol