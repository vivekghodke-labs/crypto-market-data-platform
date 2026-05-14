-- ###############################################################################
-- # Custom data test — returns rows that VIOLATE the assertion.
-- # dbt fails the test if this query returns any rows.
-- #
-- # Assertion: window_start in silver_ohlcv_validated must not be in the future
-- # relative to the dbt run time. Future timestamps indicate:
-- #   - System clock skew on the Beam worker (more than 60s drift)
-- #   - Corrupt Binance event timestamps
-- #   - Test data accidentally ingested into production tables
-- #
-- # Tolerance: 5-minute grace window to account for minor clock drift
-- # between Beam workers and the BigQuery write time.
-- ###############################################################################

select
    window_start,
    window_end,
    symbol,
    ingested_at,
    current_timestamp()         as dbt_run_time,
    timestamp_diff(
        window_start,
        current_timestamp(),
        minute
    )                           as minutes_in_future,
    'window_start is in the future' as violation_reason

from {{ ref('silver_ohlcv_validated') }}

where
    window_start > timestamp_add(current_timestamp(), interval 5 minute)