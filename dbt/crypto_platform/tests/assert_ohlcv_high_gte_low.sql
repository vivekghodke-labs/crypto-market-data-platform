-- ###############################################################################
-- # Custom data test — returns rows that VIOLATE the assertion.
-- # dbt fails the test if this query returns any rows.
-- #
-- # Assertion: For every OHLCV candle, HIGH must be >= LOW.
-- # A violation indicates a pipeline defect in the Beam aggregation logic.
-- #
-- # Applies to: silver_ohlcv_validated (validated Silver layer)
-- # If this test fails on the Silver view, investigate OHLCVCombineFn
-- # in beam/src/transforms.py — specifically the merge_accumulators() method.
-- ###############################################################################

select
    window_start,
    window_end,
    symbol,
    high,
    low,
    'high < low' as violation_reason

from {{ ref('silver_ohlcv_validated') }}

where high < low