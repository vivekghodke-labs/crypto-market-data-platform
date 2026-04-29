###############################################################################
# Null-safe division macro for financial metric calculations.
#
# Prevents ZeroDivisionError in BigQuery SQL when the denominator is zero
# or null — returns NULL instead of raising an error. NULL propagates
# correctly through all downstream aggregations (SUM, AVG treat it as absent).
#
# Usage:
#   {{ safe_divide('SUM(price * quantity)', 'SUM(quantity)') }}
#
# Produces:
#   SAFE_DIVIDE(SUM(price * quantity), SUM(quantity))
#
# Note: BigQuery's native SAFE_DIVIDE() is used under the hood. This macro
# exists to centralise the pattern and document its financial context.
###############################################################################

{% macro safe_divide(numerator, denominator) %}
    SAFE_DIVIDE({{ numerator }}, {{ denominator }})
{% endmacro %}