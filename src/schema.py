"""Enforced source schema for gfl_commercial_routes.csv.

Types are derived from direct inspection of the raw file (12,000 rows x 39 cols),
not assumed from column names. See README "Data Profile" for the evidence:
  - 6 integer columns are whole-number counts/flags (crew, stops, delay, flags)
  - 23 double columns are continuous measures (distance, fuel, money, hours, pct)
  - `date` parses cleanly as yyyy-MM-dd over 2022-01-01 .. 2024-12-31

IMPORTANT — incident_type has NO true nulls in the raw file. "No incident" is
encoded as the literal 4-char string "None" (11,703 rows); the real incident
labels are Spill / Near-Miss / Vehicle Damage (297 rows). A pandas profile hides
this because pandas' default na_values coerces the string "None" -> NaN; Spark
(and the actual file bytes) keep it verbatim. We therefore declare incident_type
nullable=True (a future load could legitimately send a real null) but DO NOT rely
on nulls to mean "no incident" — Bronze preserves the raw "None" sentinel and the
Silver layer normalizes it. See notebooks/01_bronze_ingest.ipynb DQ checks.

Bronze ingest applies this StructType so a malformed/extra/renamed column fails
fast instead of being silently inferred.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)

# Order matches the CSV header exactly. nullable=False where the profile shows
# zero nulls; only incident_type is nullable.
SOURCE_SCHEMA = StructType(
    [
        StructField("route_date_key", StringType(), False),   # PK, one row per route per day
        StructField("date", DateType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("quarter", StringType(), False),          # Q1..Q4
        StructField("day_of_week", StringType(), False),
        StructField("region", StringType(), False),           # 6 distinct
        StructField("bu", StringType(), False),               # 8 distinct
        StructField("area", StringType(), False),             # 29 distinct
        StructField("route_id", StringType(), False),         # 120 distinct (RTE-####)
        StructField("primary_waste_stream", StringType(), False),
        StructField("primary_customer_segment", StringType(), False),
        StructField("num_drivers", IntegerType(), False),
        StructField("num_trucks", IntegerType(), False),
        StructField("total_stops", IntegerType(), False),
        StructField("completed_stops", IntegerType(), False),
        StructField("missed_stops", IntegerType(), False),
        StructField("total_distance_km", DoubleType(), False),
        StructField("total_fuel_used_litres", DoubleType(), False),
        StructField("total_labour_hours", DoubleType(), False),
        StructField("total_yards", DoubleType(), False),
        StructField("total_tonnes", DoubleType(), False),
        StructField("avg_revenue_per_stop", DoubleType(), False),
        StructField("total_revenue", DoubleType(), False),
        StructField("disposal_cost", DoubleType(), False),
        StructField("fuel_cost", DoubleType(), False),
        StructField("labour_cost", DoubleType(), False),
        StructField("maintenance_cost", DoubleType(), False),
        StructField("admin_cost", DoubleType(), False),
        StructField("total_cost", DoubleType(), False),
        StructField("net_revenue", DoubleType(), False),
        StructField("gross_profit", DoubleType(), False),
        StructField("gross_margin_pct", DoubleType(), False),
        StructField("scheduled_hours", DoubleType(), False),
        StructField("actual_hours", DoubleType(), False),
        StructField("delay_minutes", IntegerType(), False),
        StructField("on_time_flag", IntegerType(), False),    # 0/1
        StructField("incident_flag", IntegerType(), False),   # 0/1
        StructField("incident_type", StringType(), True),     # raw sentinel "None" when incident_flag = 0 (not a real null)
    ]
)

# Convenience: column name list in CSV order.
SOURCE_COLUMNS = [f.name for f in SOURCE_SCHEMA.fields]
