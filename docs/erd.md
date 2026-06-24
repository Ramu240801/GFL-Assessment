# Dimensional Model — ERD

Flat **star schema** for the Route Profitability dashboard (sliceable by date,
region, BU, area). One fact at **route-month** grain, two conformed dimensions, no
surrogate keys, no SCD2 (the geography hierarchy is static and the fact is
append-mostly). DDL: [../sql/dimensional_model_ddl.sql](../sql/dimensional_model_ddl.sql).

```mermaid
erDiagram
    DIM_DATE  ||--o{ FACT_ROUTE_MONTH_PROFITABILITY : "year + month"
    DIM_ROUTE ||--o{ FACT_ROUTE_MONTH_PROFITABILITY : "route_id"

    DIM_DATE {
        int    date_month_key PK "year*100 + month  (36 rows)"
        int    year
        int    month
        string quarter
        date   month_start_date
    }
    DIM_ROUTE {
        string route_id PK "120 rows"
        string region   "denormalized geography"
        string bu       "denormalized geography"
        string area     "denormalized geography"
    }
    FACT_ROUTE_MONTH_PROFITABILITY {
        string  route_id  PK, FK "-> dim_route.route_id   (4,035 rows)"
        int     year      PK, FK "-> dim_date (year,month)"
        int     month     PK, FK
        string  quarter
        string  region    "partition key"
        string  bu        "partition key"
        string  area      "partition key"
        bigint  n_route_days
        double  total_revenue
        double  total_cost
        double  net_revenue
        double  gross_profit
        double  disposal_cost
        double  fuel_cost
        double  labour_cost
        double  maintenance_cost
        double  admin_cost
        double  total_tonnes
        double  total_yards
        double  total_distance_km
        double  total_fuel_used_litres
        double  total_labour_hours
        bigint  completed_stops
        bigint  total_stops
        bigint  missed_stops
        bigint  incident_days
        bigint  underperforming_days
        double  weighted_gross_margin_pct
        double  revenue_per_completed_stop
        double  cost_per_completed_stop
        double  profit_per_completed_stop
        double  revenue_per_tonne
        double  cost_per_tonne
        double  profit_per_tonne
        double  disposal_cost_per_tonne
        double  fuel_litres_per_100km
        double  stops_per_labour_hour
        double  completion_rate
        double  missed_stop_rate
        double  disposal_cost_share
        double  fuel_cost_share
        double  labour_cost_share
        double  maintenance_cost_share
        double  admin_cost_share
        double  pct_underperforming_days
        boolean month_underperforming_flag
    }
```

## Notes

- **Grain:** one row per `route_id × year × month` — **4,035** rows (routes don't all
  run every month, so < 120×36).
- **Keys:** natural keys only. Fact PK = `(route_id, year, month)`; `route_id` →
  `dim_route`, `(year, month)` → `dim_date` via `date_month_key = year*100 + month`.
- **Relationships are logical** (enforced in the BI tool, not by Delta — Delta does
  not enforce PK/FK constraints).
- **Partitioning** of the fact by `region, bu, area` matches the dashboard's primary
  slice keys so filters prune files. Geography is duplicated onto `dim_route` (the
  flat-star denormalization) so a single `fact → dim_route` join answers any slice.
- Counts (4,035 / 120 / 36) and column list are the live schema from notebook 03;
  the DDL is generated from these same Delta tables and verified column-for-column.
