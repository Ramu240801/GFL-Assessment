# AI Usage

Per the assignment's AI-usage requirement: what tool was used, for which parts, and
specifically where its output was reviewed, corrected, or overridden.

## Tool

**Claude Code** (Anthropic's CLI agent, Claude Opus). Used interactively, one
pipeline stage at a time, with each stage's output reviewed before moving on.

## What it was used for

- **Scaffolding** — repo structure, `src/spark_session.py` (the
  `configure_spark_with_delta_pip` builder), `src/schema.py` (the enforced
  `StructType`), and the four notebooks' cell structure.
- **Boilerplate** — repetitive PySpark transforms (the ~20 `withColumn` derived
  metrics, groupBy aggregations), the matplotlib chart code, and the pytest
  fixtures.
- **Narrative drafting** — first drafts of the markdown rationale cells, the README,
  and this file. All narrative was written **after** running the aggregations and
  reading the real output — numbers were never invented to fit prose.

## Where the AI was reviewed, corrected, or overridden

These are specific, not generic. Each is visible in the repo.

1. **`incident_type` is `"None"`, not null — the AI's first profiling pass was
   misleading.** The initial inspection used pandas, which reported `incident_type`
   as 11,703 nulls. That is wrong: the raw file contains the **literal string
   `"None"`** — pandas' default `na_values` silently coerces `"None"` → NaN, while
   Spark (and the file bytes) keep it verbatim. This was caught by a deliberate DQ
   check in notebook 01 (`incident_flag/type mismatches: 11703`), then traced to the
   sentinel. `src/schema.py`, the notebook 01 DQ check, and the markdown were all
   corrected; Bronze now preserves the raw `"None"` and Silver normalizes it. The AI
   had to override its own earlier, convenient assumption.

2. **ZORDER "no-op locally" claim — overridden as factually wrong.** A natural
   assumption (and an instruction given during the build) was that
   `OPTIMIZE … ZORDER` is Databricks-only and no-ops on open-source Spark. Rather
   than write that into the README, it was **tested**: on OSS Delta 4.0.1 the command
   **succeeded** (notebook 02: `BRANCH: ZORDER SUCCEEDED`, 54→18 files,
   `zOrderStats` populated). The README documents the true behaviour, not the common
   belief. The try/except wrapper is kept so an older engine degrades gracefully.

3. **Underperformance threshold — forced to come from data, not a suggested number.**
   The 10% `gross_margin_pct` floor was **not** accepted as a default; it was chosen
   only after computing the real distribution (P10 ≈ 9.64, median ≈ 46.99) in
   notebook 02 and reasoning explicitly against those percentiles. The route-level
   "persistent" rule was deliberately made coarser and revenue-weighted, overriding
   the simpler temptation to reuse the row-level flag.

4. **A real bug in the AI's own Spark config.** The first `build_spark()` exposed a
   custom conf key `gfl.delta.root`; Spark silently dropped it
   (`Warning: Ignoring non-Spark config property`) because non-`spark.*` keys are not
   retained. Caught on verification and fixed to `spark.gfl.delta.root`, which would
   otherwise have broken `delta_path()` at runtime.

5. **DDL provenance — required to be generated, not hand-written.** Instead of
   trusting a hand-typed `CREATE TABLE`, `sql/dimensional_model_ddl.sql` is generated
   from the live Delta schemas, then **verified column-for-column** against the built
   tables (dim_date 5/5, dim_route 4/4, fact 46/46) and test-executed to confirm it
   parses. This removed the risk of the DDL drifting from the actual tables.

6. **`requirements.txt` — taken from reality, not memory.** Initially hand-pinned;
   `pytest` turned out not to be installed at all (the first `pytest` run failed with
   `No module named pytest`). It was installed, and the file was regenerated from an
   actual `pip freeze` rather than an assumed list. The pinned pytest version was
   corrected from a guessed `8.3.4` to the real `8.4.2`.

## Net

The AI accelerated scaffolding and boilerplate substantially. The **judgment calls**
— which metrics matter for waste-route economics, where the underperformance line
sits, how the dimensional model should be shaped, and what the data actually says
about cost drivers — were verified against real computed output at every step, and
the AI was overridden wherever its first answer was convenient but wrong (items 1, 2,
and 4 especially).
