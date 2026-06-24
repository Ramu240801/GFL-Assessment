-- =====================================================================
-- GFL Route Profitability — Dimensional Model DDL (flat star schema)
-- AUTO-GENERATED from the live Delta schemas in 03_gold_and_dimensional_model.ipynb.
-- Grain of fact: one row per route per month. No surrogate keys (natural keys are
-- stable + unique). Geography denormalized onto dim_route (no snowflake / SCD2).
--
--   dim_date (date_month_key)  --+
--                                 >--  fact_route_month_profitability
--   dim_route (route_id)       --+      (route_id, year, month)
--
-- Relationships (enforced in BI tool, not by Delta):
--   fact.route_id              -> dim_route.route_id
--   (fact.year, fact.month)    -> dim_date.(year, month)   [date_month_key = year*100+month]
-- =====================================================================

-- DIMENSION: calendar at month grain. PK: date_month_key (= year*100 + month).
CREATE TABLE dim_date (
    date_month_key INT,
    year INT,
    month INT,
    quarter STRING,
    month_start_date DATE
)
USING DELTA;

-- DIMENSION: route with denormalized geography. PK: route_id.
CREATE TABLE dim_route (
    route_id STRING,
    region STRING,
    bu STRING,
    area STRING
)
USING DELTA;

-- FACT: route-month profitability. PK: (route_id, year, month). Partitioned by geography.
CREATE TABLE fact_route_month_profitability (
    route_id STRING,
    year INT,
    month INT,
    quarter STRING,
    region STRING,
    bu STRING,
    area STRING,
    n_route_days BIGINT,
    total_revenue DOUBLE,
    total_cost DOUBLE,
    net_revenue DOUBLE,
    gross_profit DOUBLE,
    disposal_cost DOUBLE,
    fuel_cost DOUBLE,
    labour_cost DOUBLE,
    maintenance_cost DOUBLE,
    admin_cost DOUBLE,
    total_tonnes DOUBLE,
    total_yards DOUBLE,
    total_distance_km DOUBLE,
    total_fuel_used_litres DOUBLE,
    total_labour_hours DOUBLE,
    completed_stops BIGINT,
    total_stops BIGINT,
    missed_stops BIGINT,
    incident_days BIGINT,
    underperforming_days BIGINT,
    weighted_gross_margin_pct DOUBLE,
    revenue_per_completed_stop DOUBLE,
    cost_per_completed_stop DOUBLE,
    profit_per_completed_stop DOUBLE,
    revenue_per_tonne DOUBLE,
    cost_per_tonne DOUBLE,
    profit_per_tonne DOUBLE,
    disposal_cost_per_tonne DOUBLE,
    fuel_litres_per_100km DOUBLE,
    stops_per_labour_hour DOUBLE,
    completion_rate DOUBLE,
    missed_stop_rate DOUBLE,
    disposal_cost_share DOUBLE,
    fuel_cost_share DOUBLE,
    labour_cost_share DOUBLE,
    maintenance_cost_share DOUBLE,
    admin_cost_share DOUBLE,
    pct_underperforming_days DOUBLE,
    month_underperforming_flag BOOLEAN
)
USING DELTA
PARTITIONED BY (region, bu, area);
