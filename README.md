# GFL Commercial — Route Profitability & Fleet Efficiency

A local, open-source **PySpark + Delta Lake** pipeline that turns raw commercial
route-day records into a reliable, sliceable view of route-level profitability.
Bronze → Silver → Gold, a flat-star dimensional model, and a profitability
analysis with a concrete operational recommendation.

> **Stack:** PySpark 4.0.3 + Delta Lake 4.0.1, run entirely locally (no Databricks,
> no Snowflake, no cloud). Every number below was produced by code that actually
> ran in this repo — each is cited to the notebook/cell it came from.

---

## 1. Setup (fresh machine)

**Prerequisites**

- **Python 3.9+** (developed on 3.9.6).
- **A JDK that Spark 4.0 supports — Java 17 or 21.** Spark 4.0 officially targets
  17/21. This repo was verified end-to-end on Java 22 (works), but for portability
  install **JDK 17 or 21** and point `JAVA_HOME` at it.
- Internet access on the **first** run only — Delta's JAR is fetched once from Maven
  Central via Ivy and then cached under `~/.ivy2`.

**Install**

```bash
# from the repo root
python3 -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt

# point Spark at a supported JDK (example for macOS / Homebrew Temurin 21)
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"   # or your JDK 17/21 path
```

`requirements.txt` is a full `pip freeze` of the verified environment. The packages
that matter directly: `pyspark==4.0.3`, `delta-spark==4.0.1`, `pandas==2.3.3`,
`matplotlib==3.9.4`, `pytest==8.4.2`, `jupyter`/`ipykernel`.

**Verify the Spark + Delta environment**

```bash
python src/spark_session.py        # launches Spark, prints version, runs spark.range(5).show()
```

**Run the pipeline, in order** (each layer reads the previous one's Delta output):

```bash
# register the venv as a Jupyter kernel once
python -m ipykernel install --user --name gfl-rp --display-name "GFL Route Profitability"

# execute notebooks top-to-bottom, in dependency order
for nb in 01_bronze_ingest 02_silver_transform 03_gold_and_dimensional_model 04_profitability_analysis; do
  jupyter nbconvert --to notebook --execute --inplace \
    --ExecutePreprocessor.kernel_name=gfl-rp notebooks/$nb.ipynb
done

# unit tests
pytest tests/ -v
```

Order matters: **01** writes Bronze from the CSV, **02** reads Bronze → Silver,
**03** reads Silver → Gold + dimensions, **04** reads Silver/Gold for analysis.
A full wipe-and-rebuild (`rm -rf delta/`) reproduces every number below identically.

**Regenerating the Delta tables.** `delta/` is **not** committed to git — it is
generated storage, fully rebuildable from the committed source CSV
(`data/raw/gfl_commercial_routes.csv`). On a fresh clone it simply won't exist yet;
running the four notebooks in order (above) recreates `delta/bronze`,
`delta/silver`, and `delta/gold` from scratch. To force a clean rebuild at any time:

```bash
rm -rf delta/          # drop all generated Bronze/Silver/Gold tables
# then re-run the four notebooks in order — they rebuild everything from the CSV
```

**Repo layout**

```
gfl-route-profitability/
├── data/raw/gfl_commercial_routes.csv        source (12,000 rows × 39 cols)
├── delta/  {bronze,silver,gold}/             local Delta lake storage
├── notebooks/  01..04                        the pipeline + analysis
├── src/  spark_session.py  schema.py  metrics.py
├── sql/  dimensional_model_ddl.sql           generated from the live Gold schemas
├── tests/  test_metrics.py                   pytest unit tests (6, all pass)
└── docs/  figures/  ai_usage.md  assignment.pdf
```

---

## 2. Metric decisions (and why — for *this* business, not generic BI)

A front-load commercial route is a **fixed-cost asset driven down a road to make
discrete billable stops**. Margin is destroyed in three specific ways, and the
Silver derived metrics ([notebook 02](notebooks/02_silver_transform.ipynb), Part A)
each target one:

- **Per-stop economics** — `revenue_/cost_/profit_per_completed_stop`. Divided by
  *completed* (not total) stops: a missed stop earns nothing but still burned drive
  time. Reveals routes that are over-served (too many cheap stops per labour hour).
- **Density / drive efficiency** — `revenue_/cost_/profit_per_km`,
  `fuel_litres_per_100km`, `stops_per_labour_hour`. Front-load profit lives on
  **route density**; a sparse route burns fuel and wages between bins.
- **Disposal drag (the waste-specific one)** — `cost_/profit_/revenue_per_tonne`
  and especially `disposal_cost_per_tonne`. Tipping fees scale with **tonnage, not
  revenue**, so a route can bill well yet bleed margin hauling heavy, low-value
  material to an expensive tip. This is the metric generic BI misses.
- **Operational health** — `completion_rate`, `missed_stop_rate`, and the
  **cost-component shares** (`disposal/fuel/labour/maintenance/admin_cost_share`),
  so "what drove this thin day" is a direct lookup.

All ratios use a **`safe_div` helper that returns null on a zero/null denominator**
([src/metrics.py](src/metrics.py)) — no divide-by-zero poison from a light day with
0 tonnes. Margin at every grain is **revenue-weighted** (Σprofit / Σrevenue), never
a mean of daily percentages.

A real data-fidelity catch worth noting ([notebook 01](notebooks/01_bronze_ingest.ipynb)
DQ checks): `incident_type` has **zero true nulls** — "no incident" is the literal
string `"None"` (11,703 rows). A pandas profile hides this (pandas coerces `"None"`
→ NaN); Spark keeps it verbatim. Bronze preserves the raw sentinel; Silver
normalizes it to null (297 real incidents remain).

---

## 3. Underperformance definition (reasoned from real percentiles)

**A route-day is underperforming when `gross_margin_pct < 10%`.**

Chosen from the **actual** `gross_margin_pct` distribution computed over all 12,000
route-days ([notebook 02](notebooks/02_silver_transform.ipynb), Part B):

| P10 | P25 | median | P75 | P90 | mean | negative gross_profit |
|---|---|---|---|---|---|---|
| **9.64** | 28.51 | 46.99 | 67.35 | 76.14 | 44.75 | 717 rows (5.97%) |

Reasoning, grounded in those numbers:

- **It tracks the natural break, not a round guess.** P10 ≈ 9.6%, so a 10% cut
  isolates almost exactly the **worst decile** while leaving the healthy middle
  (median ~47%) untouched. A cut near P25 (28%) would brand a quarter of normal
  routes as failing and drown the signal.
- **It fully contains the bleed.** Every negative-profit day (~6%) is below 10%, so
  the flag captures all loss-makers **plus** the fragile 0–10% band — where one fuel
  tick or one reweighed heavy load flips the day negative.
- **It's an ops-actionable floor.** Below ~10% gross margin a route-day contributes
  almost nothing to fixed overhead; at/above it the route carries its weight.

**Route-level "persistent underperformer"** is a deliberately **coarser, separate**
rule (one bad day ≠ a structurally unprofitable route): a `route_id` is persistent
when **revenue-weighted margin across all its days < 10% OR ≥ 20% of its days are
individually below 10%**. Revenue-weighting prevents one big cheap day hiding behind
many tiny good ones.

**Counts under the chosen rule** ([notebook 02](notebooks/02_silver_transform.ipynb), Part B, Step 5):
**1,216 / 12,000 underperforming route-days (10.13%)**, and **21 / 120 persistent
routes (17.5%)**.

---

## 4. Key findings

All figures from the validated final run ([notebook 04](notebooks/04_profitability_analysis.ipynb)).

1. **Fleet is profitable in aggregate but margin is concentrated.** 3-year totals:
   **$99,600,429 revenue**, **$50,715,180 cost**, **$48,885,249 gross profit →
   49.08% weighted margin**. (notebook 04, Part 1 / validation pass.)

2. **Underperformance is concentrated, not diffuse.** **Calgary BU** is the laggard
   at **36.51% weighted margin** (15.4% of its days flagged) vs 46–56% for every
   other BU. Worst areas: **Airdrie 11.28%, Laval 12.37%, Kingston 12.52%, Toronto
   East 17.96%, Toronto North 20.76%**. (notebook 04, Part 1 —
   `docs/figures/01_underperformance_by_hierarchy.png`.)

3. **The driver is material economics + underpricing — NOT crew productivity.** On
   underperforming vs healthy days: disposal is **74.7% vs 58.1%** of cost,
   disposal **$82.59 vs $59.17 per tonne**, revenue **$36.10 vs $62.33 per stop** —
   a bad day bills *less* per stop than it costs. **Completion rate is identical
   (0.985)** in both groups: these routes aren't missing stops, they're hauling
   heavy low-value material at prices that don't cover the gate fee. By stream,
   **General Waste (37.29% margin)** is the drag vs Cardboard (77.80%). (notebook 04,
   Part 2 — `docs/figures/02_cost_drivers.png`.)

4. **Three-year trend is gently improving, not deteriorating.** Weighted margin
   **48.27% → 49.11% → 49.78%** (2022→2024); profit per stop **$27.46 → $29.24 →
   $30.78**. The worst pockets above persist, masked by the healthy majority.
   (notebook 04, Part 3 — `docs/figures/03_three_year_trend.png`.)

5. **The worst pockets are one specific segment.** **Office & Commercial** dominates
   the worst area×segment list — **Fredericton / Office & Commercial is negative at
   −13.00% weighted margin** — followed by Airdrie, Laval, Kingston, Regina (all
   Office & Commercial). (notebook 04, Part 4 — `docs/figures/04_worst_pockets.png`.)

### Recommendation for next quarter

**Re-price and re-route the ~21 persistent Office & Commercial / General-Waste routes
in Calgary BU (Airdrie) and the Fredericton, Laval, Kingston, Regina areas.** These
route-days collect heavy General Waste at **~$83/tonne disposal** but bill only
**~$36/stop** against a **~$38/stop cost** — a structural pricing gap, since
completion is already 98.5% (no productivity slack to cut). Priority moves: (1) pass
through a disposal surcharge / re-rate those contracts to close the per-stop gap;
(2) divert recoverable cardboard/recycling (73–78% margin) out of the General-Waste
stream to cut tonnage to the expensive tip; (3) consolidate the low-density
Airdrie/Fredericton days. The recoverable margin is in lifting these 21 routes from
sub-10% toward the ~47% BU median — a commercial/pricing fix, not a service-quality one.

---

## 5. Data model

Flat **star schema** ([notebook 03](notebooks/03_gold_and_dimensional_model.ipynb),
DDL in [sql/dimensional_model_ddl.sql](sql/dimensional_model_ddl.sql)):

```
   dim_date (date_month_key)  --+
                                 >--  fact_route_month_profitability
   dim_route (route_id)       --+      (route_id, year, month)
```

- **Grain of the fact: route × month** — **4,035 rows** (not the full 4,320; routes
  don't all run every month). ~3× smaller than Silver, monthly trend preserved,
  route identity preserved. Route-day is too noisy/heavy to serve; route-only kills
  the time axis needed for the 3-year question.
- **Flat star, no surrogate keys, no SCD2** — the geography hierarchy is static (the
  >1-geo-per-route check returns 0) and the fact is append-mostly, so snowflake +
  SCD2 machinery would model change that can't happen here. One join per slice.
- The DDL file is **generated from the live Delta schemas** and verified
  column-for-column against the built tables (dim_date 5/5, dim_route 4/4, fact
  46/46 — all match).

---

## 6. Delta Lake features used

- **Enforced schema + FAILFAST** (Bronze) — a future schema drift fails loudly
  instead of silently coercing to null.
- **MERGE** (Bronze, keyed on `route_date_key`) — the right primitive for this data:
  disposal invoices and billing corrections **restate** existing route-days days/
  weeks later. MERGE upserts those late/restated rows idempotently; `append` would
  duplicate the key, `overwrite` would rewrite correct history. This is also *why
  the dimensional layer needs no SCD2* — mutation is absorbed upstream.
- **Partitioning** — Bronze/Silver by time (`year`/`month`, `region`), Gold by the
  dashboard's slice keys (`region/bu/area`) so leadership filters prune files.
- **OPTIMIZE … ZORDER BY (bu, area, route_id)** (Silver) — co-locates the
  drill-down keys within partitions.
  **Note on ZORDER:** it is *commonly believed* to be Databricks-only. That is
  **outdated** — open-source Delta Lake has supported `OPTIMIZE … ZORDER` since
  Delta 2.0, and it **ran successfully here on OSS Delta 4.0.1**: notebook 02
  reported `BRANCH: ZORDER SUCCEEDED`, compacting 54 → 18 files with populated
  `zOrderStats`. The command is wrapped in try/except so that on an *older* engine
  that genuinely lacks it, the notebook degrades to a plain `OPTIMIZE` compaction and
  says so, rather than failing. In a production Databricks deployment ZORDER would
  matter even more (larger data, Photon-aware skipping); here it already works.
- **Time travel** — full `overwrite` rebuilds of Silver/Gold create new versions;
  `DESCRIBE HISTORY` + `VERSION AS OF` let you reproduce a past dashboard state or
  diff a restated closed month for audit.
- **Schema evolution** — `overwriteSchema=true` lets new derived metrics land in
  Silver/Gold without a manual migration, while the enforced Bronze contract keeps
  raw ingestion strict.

---

## 7. Tests

`pytest tests/ -v` → **6 passed**. Covers the `safe_div` helper (zero denominator,
null denominator, normal division, Column-expression input) and the underperformance
flag (negative margin flagged; strict-`<` boundary: exactly 10.0 is *not* flagged,
9.99 is). See [tests/test_metrics.py](tests/test_metrics.py); logic under test lives
in [src/metrics.py](src/metrics.py) — the same code the notebooks use.

See [docs/ai_usage.md](docs/ai_usage.md) for AI-tool usage and where its output was
reviewed/overridden.
