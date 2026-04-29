###############################################################################
# Materialisation: table (full refresh, partitioned by trade_date)
# Target: gold_analytics.gold_moving_averages
#
# Purpose:
#   Computes 7-day and 30-day Simple Moving Averages of the daily BTC/USDT
#   close price. One row per (symbol, trade_date).
#
# SMA Formula (industry standard):
#   SMA(N) = AVG(close) OVER (
#       PARTITION BY symbol
#       ORDER BY trade_date ASC
#       ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW
#   )
#
# NULL handling:
#   - sma_7d  is NULL when fewer than 7 days of history exist in the window.
#   - sma_30d is NULL when fewer than 30 days of history exist in the window.
#   - sma_Nd_valid flags whether the SMA value is statistically meaningful.
#   - Looker Studio omits NULL data points from line charts — no misleading
#     zero values are rendered during the warm-up period.
#
# Trend signal:
#   above_both → close > sma_7d AND close > sma_30d  (bullish — price above both MAs)
#   below_both → close < sma_7d AND close < sma_30d  (bearish — price below both MAs)
#   mixed      → any other combination               (transitional / consolidation)
#
# Primary consumers:
#   - Looker Studio Page 1: Price Overview (SMA overlay line chart)
#   - Looker Studio scorecards: current SMA_7d and SMA_30d values
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

daily_closes as (

    select
        trade_date,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        trade_count,
        price_change_pct,
        candle_direction

    from {{ ref('gold_daily_ohlcv') }}

),

-- Count available rows of history per symbol up to each date.
-- Used to determine whether an SMA is statistically valid.
history_counts as (

    select
        trade_date,
        symbol,
        close,
        -- Number of rows available in the window (including current row)
        count(*) over (
            partition by symbol
            order by trade_date asc
            rows between unbounded preceding and current row
        ) as cumulative_row_count

    from daily_closes

),

sma_calculations as (

    select
        dc.trade_date,
        dc.symbol,
        dc.open,
        dc.high,
        dc.low,
        dc.close,
        dc.volume,
        dc.trade_count,
        dc.price_change_pct,
        dc.candle_direction,
        hc.cumulative_row_count,

        -- 7-Day Simple Moving Average
        -- ROWS BETWEEN 6 PRECEDING AND CURRENT ROW = window of 7 rows
        avg(dc.close) over (
            partition by dc.symbol
            order by dc.trade_date asc
            rows between 6 preceding and current row
        )                                               as sma_7d_raw,

        -- 30-Day Simple Moving Average
        avg(dc.close) over (
            partition by dc.symbol
            order by dc.trade_date asc
            rows between 29 preceding and current row
        )                                               as sma_30d_raw,

        -- Row count in each SMA window — used to null out partial windows
        count(dc.close) over (
            partition by dc.symbol
            order by dc.trade_date asc
            rows between 6 preceding and current row
        )                                               as sma_7d_window_size,

        count(dc.close) over (
            partition by dc.symbol
            order by dc.trade_date asc
            rows between 29 preceding and current row
        )                                               as sma_30d_window_size

    from daily_closes dc
    inner join history_counts hc
        on  dc.trade_date = hc.trade_date
        and dc.symbol     = hc.symbol

),

final as (

    select
        trade_date,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        trade_count,
        price_change_pct,
        candle_direction,

        -- SMA 7d: NULL when window is not yet full (< 7 days of history)
        case
            when sma_7d_window_size = 7 then round(sma_7d_raw, 8)
            else null
        end                                             as sma_7d,

        -- SMA 30d: NULL when window is not yet full (< 30 days of history)
        case
            when sma_30d_window_size = 30 then round(sma_30d_raw, 8)
            else null
        end                                             as sma_30d,

        -- Validity flags — Looker Studio can filter on these
        (sma_7d_window_size  = 7)                       as sma_7d_valid,
        (sma_30d_window_size = 30)                      as sma_30d_valid,

        -- Momentum signals: price position relative to each SMA
        -- NULL when the respective SMA is not yet valid
        case
            when sma_7d_window_size = 7
            then {{ safe_divide('(close - sma_7d_raw)', 'sma_7d_raw') }} * 100
            else null
        end                                             as price_vs_sma_7d_pct,

        case
            when sma_30d_window_size = 30
            then {{ safe_divide('(close - sma_30d_raw)', 'sma_30d_raw') }} * 100
            else null
        end                                             as price_vs_sma_30d_pct,

        -- Trend signal: requires both SMAs to be valid for a meaningful signal
        case
            when sma_7d_window_size  = 7
             and sma_30d_window_size = 30
            then
                case
                    when close > sma_7d_raw  and close > sma_30d_raw then 'above_both'
                    when close < sma_7d_raw  and close < sma_30d_raw then 'below_both'
                    else 'mixed'
                end
            else 'insufficient_history'
        end                                             as trend_signal,

        -- Golden cross / death cross detection
        -- Golden cross: sma_7d crosses ABOVE sma_30d (strong bullish signal)
        -- Death cross:  sma_7d crosses BELOW sma_30d (strong bearish signal)
        case
            when sma_7d_window_size  = 7
             and sma_30d_window_size = 30
            then
                case
                    when sma_7d_raw > sma_30d_raw then 'golden_cross_zone'
                    when sma_7d_raw < sma_30d_raw then 'death_cross_zone'
                    else 'at_crossover'
                end
            else null
        end                                             as cross_signal,

        cumulative_row_count                            as days_of_history,
        current_timestamp()                             as dbt_updated_at

    from sma_calculations

)

select * from final
order by trade_date desc, symbol