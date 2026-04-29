###############################################################################
# Materialisation: incremental (merge on trade_id)
# Target: silver_curated.silver_deduped_trades
#
# Purpose:
#   Deduplicates raw trade events from bronze_raw.raw_trades.
#   The Bronze layer may contain duplicate trade_ids because:
#     1. The Beam pipeline uses ACCUMULATING window mode — a window can fire
#        multiple times (speculative early + final), writing the same trades
#        more than once to Bronze.
#     2. Network retries from the WebSocket ingestor under reconnection.
#
# Deduplication strategy:
#   ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY ingested_at ASC)
#   — keeps the first-ingested record for each trade_id.
#   This is deterministic and idempotent on re-runs.
#
# Incremental logic:
#   On the first run: full scan of bronze_raw.raw_trades.
#   On subsequent runs: only processes rows where ingested_at > max(ingested_at)
#   in the current silver table. BigQuery merge on trade_id handles any late
#   duplicates that arrive in the incremental window.
#
# Partition: trade_date (DATE derived from trade_time_ms) — enables
#   cost-efficient date-range queries in Gold models.
###############################################################################

{{
    config(
        materialized        = 'incremental',
        unique_key          = 'trade_id',
        incremental_strategy = 'merge',
        partition_by        = {
            'field': 'trade_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by          = ['symbol'],
        on_schema_change    = 'fail'
    )
}}

with

source_trades as (

    select
        trade_id,
        symbol,
        price,
        quantity,
        trade_time_ms,
        event_time_ms,
        is_market_maker,
        ingested_at,

        -- Derive a DATE partition column from millisecond timestamp
        date(
            timestamp_millis(trade_time_ms)
        ) as trade_date,

        -- Derive a TIMESTAMP for direct time-series queries
        timestamp_millis(trade_time_ms) as trade_timestamp

    from {{ source('bronze_raw', 'raw_trades') }}

    {% if is_incremental() %}
        -- Incremental filter: only process new rows since last successful run.
        -- Uses ingested_at (pipeline write time) not trade_time_ms (event time)
        -- because late-arriving events may have old event times but new ingestion times.
        where ingested_at > (
            select max(ingested_at)
            from {{ this }}
        )
    {% endif %}

),

deduplicated as (

    select
        trade_id,
        symbol,
        price,
        quantity,
        trade_time_ms,
        event_time_ms,
        is_market_maker,
        ingested_at,
        trade_date,
        trade_timestamp,

        -- Rank duplicates: keep the first record ingested for each trade_id.
        -- In a production multi-region setup, ties are broken by event_time_ms.
        row_number() over (
            partition by trade_id
            order by ingested_at asc, event_time_ms asc
        ) as _dedup_rank

    from source_trades

),

final as (

    select
        trade_id,
        symbol,
        price,
        quantity,
        trade_time_ms,
        event_time_ms,
        is_market_maker,
        ingested_at,
        trade_date,
        trade_timestamp

    from deduplicated
    where _dedup_rank = 1

)

select * from final