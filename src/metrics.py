"""Canonical, testable profitability-metric helpers.

These are the exact implementations used inline in the Silver/Gold notebooks
(02, 03). Kept here so they can be unit-tested independently of the Delta
pipeline. See tests/test_metrics.py.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Underperformance floor for gross_margin_pct, derived from the real P10 of the
# data (~9.6%) in notebook 02 — a route-day below this is structurally fragile.
UNDERPERFORMANCE_THRESHOLD = 10.0


def safe_div(num, den) -> Column:
    """Divide two numeric columns, returning NULL when the denominator is NULL or
    zero (instead of throwing or producing inf/NaN).

    Accepts either column names (str) or Column expressions for both operands.
    """
    num_c = F.col(num) if isinstance(num, str) else num
    den_c = F.col(den) if isinstance(den, str) else den
    return F.when((den_c.isNull()) | (den_c == 0), None).otherwise(num_c / den_c)


def add_underperforming_flag(
    df,
    margin_col: str = "gross_margin_pct",
    threshold: float = UNDERPERFORMANCE_THRESHOLD,
    out_col: str = "underperforming_flag",
):
    """Add a boolean underperformance flag: margin STRICTLY below the threshold.

    Strict ``<`` is deliberate — a route-day sitting exactly at the threshold is
    treated as just-passing, not failing (boundary behaviour covered by tests).
    """
    return df.withColumn(out_col, F.col(margin_col) < threshold)
