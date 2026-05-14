-- ###############################################################################
-- # Custom data test — returns rows that VIOLATE the assertion.
-- # dbt fails the test if this query returns any rows.
-- #
-- # Assertion: sma_7d must be NULL on days where fewer than 7 days of history
-- # exist AND must be non-NULL on days where 7 or more days of history exist.
-- #
-- # This test guards against a regression where partial-window SMA values
-- # (computed from fewer than N data points) are incorrectly exposed as valid
-- # SMAs. A partial-window SMA would be numerically misleading — it would
-- # represent an average of 3 or 4 days and be labelled as a "7-day MA".
-- #
-- # Two violation types detected:
-- #   Type A: sma_7d is NOT NULL but days_of_history < 7  (false positive SMA)
-- #   Type B: sma_7d IS NULL but days_of_history >= 7     (missing valid SMA)
-- ###############################################################################

-- Type A: SMA-7 populated but insufficient history (< 7 days)
select
    trade_date,
    symbol,
    sma_7d,
    days_of_history,
    'sma_7d_non_null_with_insufficient_history' as violation_type

from {{ ref('gold_moving_averages') }}

where
    sma_7d is not null
    and days_of_history < 7

union all

-- Type B: SMA-7 is NULL despite having sufficient history (>= 7 days)
select
    trade_date,
    symbol,
    sma_7d,
    days_of_history,
    'sma_7d_null_with_sufficient_history' as violation_type

from {{ ref('gold_moving_averages') }}

where
    sma_7d is null
    and days_of_history >= 7