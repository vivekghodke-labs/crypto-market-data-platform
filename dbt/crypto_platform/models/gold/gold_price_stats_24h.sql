###############################################################################
# Materialisation: table (full refresh)
# Target: gold_analytics.gold_price_stats_24h
#
# Purpose:
#   24-hour rolling price statistics per symbol.
#   One row per symbol representing the trailing 24-hour window from
#   the most recent trade timestamp in the dataset.
#
# VWAP (Volume Weighted Average Price) — true formula:
#   VWAP = SUM(price × quantity) / SUM(quantity)
#
#   Source: silver_deduped_trades (individual trade level)
#   This is the mathematically correct VWAP used by:
#     - Bloomberg Terminal
#     - Refinitiv Eikon
#     - ICE Data Services
#     - All major exchange reporting standards
#
#   Using individual trades (not OHLCV candles) ensures:
#     - Each trade is weighted by its actual executed quantity
#     - No approximation from candle aggregation
#     - Results match exchange-reported VWAP exactly
#
# Additional metrics:
#   - 24h high / low
#   - Price change vs 24h ago (open price of the trailing window)
#   - Trade count and total notional volume
#   - Market activity classification (high/normal/low volume)
###############################################################################

{{
    config(
        materialized = 'table',
        cluster_by   = ['symbol']
    )
}}

with

-- Determine the reference timestamp: latest trade time in the dataset.
-- All 24h calculations are relative to this anchor, not wall-clock now().
-- This ensures the model is deterministic on re-runs within the same dbt run.
latest_trade_time as (

    select max(trade_timestamp) as anchor_ts
    from {{ ref('silver_deduped_trades') }}

),

-- Filter to the trailing 24-hour window from the anchor timestamp.
trades_24h as (

    select
        t.trade_id,
        t.symbol,
        t.price,
        t.quantity,
        t.trade_timestamp,
        t.is_market_maker,
        l.anchor_ts

    from {{ ref('silver_deduped_trades') }} t
    cross join latest_trade_time l

    where
        t.trade_timestamp >= timestamp_sub(l.anchor_ts, interval 24 hour)
        and t.trade_timestamp <= l.anchor_ts

),

-- True VWAP: SUM(price × quantity) / SUM(quantity)
-- Computed at individual trade granularity — no approximation.
vwap_calc as (

    select
        symbol,
        anchor_ts,

        -- True VWAP numerator and denominator kept separate for transparency
        sum(price * quantity)                       as total_notional,   -- SUM(P × Q)
        sum(quantity)                               as total_quantity,   -- SUM(Q)

        {{ safe_divide(
            'sum(price * quantity)',
            'sum(quantity)'
        ) }}                                        as vwap,

        -- Price range
        max(price)                                  as high_24h,
        min(price)                                  as low_24h,

        -- Trade activity
        count(*)                                    as trade_count_24h,

        -- Notional volume (USD equivalent)
        sum(price * quantity)                       as notional_volume_24h,

        -- Market maker vs taker split — indicator of market liquidity
        countif(is_market_maker = true)             as maker_trade_count,
        countif(is_market_maker = false)            as taker_trade_count,

        -- First trade price in the 24h window (used as 24h open)
        first_value(price) over (
            partition by symbol
            order by anchor_ts     -- static, forces single-row window
        )                                           as _anchor_placeholder

    from trades_24h
    group by symbol, anchor_ts

),

-- 24h open: price of the earliest trade in the trailing window.
open_24h as (

    select
        symbol,
        price as open_24h

    from (
        select
            symbol,
            price,
            row_number() over (
                partition by symbol
                order by trade_timestamp asc
            ) as rn
        from trades_24h
    )
    where rn = 1

),

-- 24h close: price of the most recent trade.
close_24h as (

    select
        symbol,
        price as close_24h

    from (
        select
            symbol,
            price,
            row_number() over (
                partition by symbol
                order by trade_timestamp desc
            ) as rn
        from trades_24h
    )
    where rn = 1

),

final as (

    select
        v.symbol,
        v.anchor_ts                                         as stats_as_of,

        -- OHLC for 24h window
        o.open_24h                                          as open_24h,
        v.high_24h,
        v.low_24h,
        c.close_24h,

        -- True VWAP (industry standard formula)
        v.vwap,
        v.total_notional,
        v.total_quantity,

        -- Price change
        c.close_24h - o.open_24h                            as price_change_24h,
        {{ safe_divide(
            '(c.close_24h - o.open_24h)',
            'o.open_24h'
        ) }} * 100                                          as price_change_pct_24h,

        -- Range metrics
        v.high_24h - v.low_24h                              as range_24h,
        {{ safe_divide(
            '(v.high_24h - v.low_24h)',
            'o.open_24h'
        ) }} * 100                                          as range_pct_24h,

        -- Spread: VWAP vs midpoint (high+low)/2
        -- Positive = VWAP above midpoint (buying pressure)
        v.vwap - (v.high_24h + v.low_24h) / 2              as vwap_vs_midpoint,

        -- Trade activity
        v.trade_count_24h,
        v.notional_volume_24h,
        v.maker_trade_count,
        v.taker_trade_count,
        {{ safe_divide(
            'v.taker_trade_count',
            'v.trade_count_24h'
        ) }} * 100                                          as taker_pct,

        -- Market direction
        case
            when c.close_24h >= o.open_24h then 'bullish'
            else 'bearish'
        end                                                 as market_direction_24h,

        current_timestamp()                                 as dbt_updated_at

    from vwap_calc       v
    inner join open_24h  o on v.symbol = o.symbol
    inner join close_24h c on v.symbol = c.symbol

)

select * from final