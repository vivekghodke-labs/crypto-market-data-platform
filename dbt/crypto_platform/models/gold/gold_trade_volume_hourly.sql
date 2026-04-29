###############################################################################
# Materialisation: table (full refresh, partitioned by trade_hour)
# Target: gold_analytics.gold_trade_volume_hourly
#
# Purpose:
#   Hourly trade volume and activity metrics per symbol.
#   One row per (symbol, trade_hour UTC).
#
# Source: silver_deduped_trades (individual trade level) — ensures exact
#   volume figures with no approximation from OHLCV candle aggregation.
#
# Primary consumers:
#   - Looker Studio hourly volume bar chart
#   - Intraday liquidity pattern analysis
#   - Operational monitoring (volume anomaly detection)
###############################################################################

{{
    config(
        materialized = 'table',
        partition_by = {
            'field': 'trade_hour',
            'data_type': 'timestamp',
            'granularity': 'hour'
        },
        cluster_by = ['symbol']
    )
}}

with

hourly_trades as (

    select
        symbol,

        -- Truncate trade_timestamp to the hour boundary
        timestamp_trunc(trade_timestamp, hour)      as trade_hour,

        price,
        quantity,
        is_market_maker,
        trade_id

    from {{ ref('silver_deduped_trades') }}

),

aggregated as (

    select
        symbol,
        trade_hour,

        -- Volume metrics
        sum(price * quantity)                       as notional_volume,     -- USD equivalent
        sum(quantity)                               as btc_volume,          -- BTC quantity

        -- True VWAP for the hour (same formula as 24h model)
        {{ safe_divide(
            'sum(price * quantity)',
            'sum(quantity)'
        ) }}                                        as vwap_hourly,

        -- Price range within the hour
        max(price)                                  as high,
        min(price)                                  as low,
        max(price) - min(price)                     as price_range,

        -- Activity metrics
        count(*)                                    as trade_count,
        countif(is_market_maker = true)             as maker_count,
        countif(is_market_maker = false)            as taker_count,

        -- Average trade size
        {{ safe_divide('sum(quantity)', 'count(*)') }}
                                                    as avg_trade_size_btc,
        {{ safe_divide('sum(price * quantity)', 'count(*)') }}
                                                    as avg_trade_size_usd

    from hourly_trades
    group by 1, 2

),

final as (

    select
        symbol,
        trade_hour,

        -- Convenience date/time columns for Looker Studio filters
        date(trade_hour)                            as trade_date,
        extract(hour from trade_hour)               as hour_of_day,
        format_timestamp('%A', trade_hour)          as day_of_week,

        -- Volume
        notional_volume,
        btc_volume,
        vwap_hourly,

        -- Price range
        high,
        low,
        price_range,
        {{ safe_divide('price_range', 'low') }} * 100
                                                    as price_range_pct,

        -- Activity
        trade_count,
        maker_count,
        taker_count,
        {{ safe_divide('taker_count', 'trade_count') }} * 100
                                                    as taker_pct,
        avg_trade_size_btc,
        avg_trade_size_usd,

        current_timestamp()                         as dbt_updated_at

    from aggregated

)

select * from final
order by trade_hour desc, symbol