"""Unit tests for src/metrics.py — safe_div and the underperformance flag.

Uses a lightweight local SparkSession (no Delta needed) and small synthetic
DataFrames so each edge case is explicit: zero/null denominator, negative
profit, and the boundary value exactly at the threshold.

Run:  JAVA_HOME=<jdk> pytest tests/ -v
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from metrics import safe_div, add_underperforming_flag, UNDERPERFORMANCE_THRESHOLD  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_metrics")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _rows(spark, data, cols):
    return spark.createDataFrame(data, cols)


# --------------------------------------------------------------------------- #
# safe_div
# --------------------------------------------------------------------------- #
def test_safe_div_zero_denominator_returns_null(spark):
    """0 denominator -> null, NOT an exception or infinity."""
    df = _rows(spark, [(10.0, 0.0), (20.0, 0.0)], ["num", "den"])
    out = df.withColumn("r", safe_div("num", "den")).select("r").collect()
    assert all(row["r"] is None for row in out)


def test_safe_div_null_denominator_returns_null(spark):
    """null denominator -> null."""
    schema = StructType([
        StructField("num", DoubleType(), True),
        StructField("den", DoubleType(), True),
    ])
    df = spark.createDataFrame([(10.0, None), (5.0, None)], schema)
    out = df.withColumn("r", safe_div("num", "den")).select("r").collect()
    assert all(row["r"] is None for row in out)


def test_safe_div_normal_division_is_correct(spark):
    """Non-zero denominator -> exact quotient."""
    df = _rows(spark, [(10.0, 4.0), (9.0, 3.0)], ["num", "den"])
    out = [r["r"] for r in df.withColumn("r", safe_div("num", "den")).select("r").collect()]
    assert out == pytest.approx([2.5, 3.0])


def test_safe_div_accepts_column_expressions(spark):
    """safe_div should accept Column expressions, not just names (used for
    fuel_litres_per_100km = fuel*100 / distance)."""
    df = _rows(spark, [(2.0, 50.0)], ["fuel", "dist"])
    out = df.withColumn("r", safe_div(F.col("fuel") * 100, "dist")).select("r").first()["r"]
    assert out == pytest.approx(4.0)


# --------------------------------------------------------------------------- #
# underperformance flag
# --------------------------------------------------------------------------- #
def test_underperforming_flag_negative_profit_is_flagged(spark):
    """A clearly loss-making margin (negative) is flagged True."""
    df = _rows(spark, [("a", -30.5), ("b", -0.3)], ["route_date_key", "gross_margin_pct"])
    res = {r["route_date_key"]: r["underperforming_flag"]
           for r in add_underperforming_flag(df).collect()}
    assert res == {"a": True, "b": True}


def test_underperforming_flag_boundary_is_strict(spark):
    """Boundary: exactly at the threshold (10.0) is NOT flagged (strict <);
    just below IS; well above is NOT."""
    t = UNDERPERFORMANCE_THRESHOLD
    df = _rows(
        spark,
        [("at", t), ("below", t - 0.01), ("above", t + 0.01), ("healthy", 47.0)],
        ["route_date_key", "gross_margin_pct"],
    )
    res = {r["route_date_key"]: r["underperforming_flag"]
           for r in add_underperforming_flag(df).collect()}
    assert res["at"] is False        # exactly 10.0 -> just passing
    assert res["below"] is True      # 9.99 -> flagged
    assert res["above"] is False     # 10.01 -> not flagged
    assert res["healthy"] is False   # 47 -> not flagged
