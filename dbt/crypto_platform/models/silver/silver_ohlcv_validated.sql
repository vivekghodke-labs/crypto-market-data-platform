###############################################################################
# Materialisation: view
# Target: silver_curated.silver_ohlcv_validated
#
# Purpose:
#   A validation and deduplication layer on top of the Beam-written
#   ohlcv_1min table. Addresses two concerns:
#
#   1. Duplicate windows: Beams ACCUMULATING mode can fire a window
#      multiple times (early speculative panes + final pane). Each firing
#      writes a new row to ohlcv_1min with the same window_start/symbol.
#      This view keeps only the most recent pane per window (latest ingested_at).
#
#   2. Data quality assertions: Filters rows that violate OHLCV invariants
#      (e.g., high < low) — these represent pipeline defects, not valid data.
#      Filtered rows are visible in the custom data tests for alerting.
#
# Materialised as a VIEW (not table) because:
#   - ohlcv_1min is already partitioned and clustered — BigQuery scans are cheap.
#   - Gold models query specific date partitions — partition pruning applies
#     even through the view layer in BigQuery.
#   - No storage duplication of Silver data.
###############################################################################

{{
    config(
        materialized = 'view'
    )
}}

with

source_ohlcv as (

    select
        window_start,
        window_end,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        trade_count,
        ingested_at,

        -- Deduplicate: for each (symbol, window_start) pair, keep only the
        -- row from the most recent Beam pane (highest ingested_at).
        -- This is the final, most complete aggregation for that window.
        row_number() over (
            partition by symbol, window_start
            order by ingested_at desc
        ) as _pane_rank

    from {{ source('silver_beam', 'ohlcv_1min') }}

),

-- Apply OHLCV invariant constraints.
-- Rows violating these constraints indicate a pipeline defect and are
-- excluded from Gold models. They are surfaced by custom data tests.
validated as (

    select
        window_start,
        window_end,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        trade_count,
        ingested_at,

        -- Derived fields used by Gold models
        date(window_start)              as candle_date,
        extract(hour from window_start) as candle_hour,

        -- Candle body metrics — useful for technical analysis Gold models
        abs(close - open)               as candle_body_size,
        high - low                      as candle_wick_range,

        case
            when close >= open then 'bullish'
            when close < open  then 'bearish'
        end                             as candle_direction

    from source_ohlcv

    where
        -- Keep only the most recent pane per window
        _pane_rank = 1

        -- OHLCV structural invariants
        and high >= low
        and high >= open
        and high >= close
        and low  <= open
        and low  <= close
        and volume > 0
        and trade_count > 0

)

select * from validated