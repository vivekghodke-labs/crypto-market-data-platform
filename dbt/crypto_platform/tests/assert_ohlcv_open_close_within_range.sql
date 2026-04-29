###############################################################################
# Custom data test — returns rows that VIOLATE the assertion.
# dbt fails the test if this query returns any rows.
#
# Assertion: OPEN and CLOSE must both be within [LOW, HIGH] bounds.
# This is a fundamental OHLCV invariant — open and close are prices that
# occurred during the window, so they cannot exceed the window high or
# fall below the window low.
#
# Violations indicate:
#   - A bug in OHLCVCombineFn.add_input() or merge_accumulators()
#   - Incorrect timestamp ordering causing wrong open/close assignment
###############################################################################

select
    window_start,
    window_end,
    symbol,
    open,
    high,
    low,
    close,
    case
        when open  > high then 'open > high'
        when open  < low  then 'open < low'
        when close > high then 'close > high'
        when close < low  then 'close < low'
    end as violation_reason

from {{ ref('silver_ohlcv_validated') }}

where
    open  > high
    or open  < low
    or close > high
    or close < low