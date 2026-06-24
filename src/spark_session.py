"""Shared local Delta-enabled SparkSession builder.

Uses `configure_spark_with_delta_pip`, the supported way to run Delta Lake on a
plain pip-installed PySpark (no manual JAR downloads / classpath wrangling). The
helper resolves the Delta Maven coordinate that matches the installed
`delta-spark` version and injects it via `spark.jars.packages`; Ivy fetches it on
first launch (needs network once, then cached under ~/.ivy2).

All Delta tables in this project are plain filesystem paths under ./delta, so no
external metastore is required.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Repo root = parent of this src/ directory. Delta storage lives at <root>/delta.
REPO_ROOT = Path(__file__).resolve().parent.parent
DELTA_ROOT = REPO_ROOT / "delta"


def build_spark(
    app_name: str = "gfl-route-profitability",
    delta_root: str | os.PathLike | None = None,
) -> SparkSession:
    """Build (or get) a local Delta-enabled SparkSession.

    Parameters
    ----------
    app_name:
        Spark application name shown in the UI / logs.
    delta_root:
        Base directory for Delta storage. Defaults to <repo>/delta. Exposed as a
        Spark conf `gfl.delta.root` so notebooks can resolve bronze/silver/gold
        paths without hardcoding.
    """
    root = Path(delta_root) if delta_root is not None else DELTA_ROOT
    root.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Delta SQL extensions + catalog
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Local-friendly defaults: small shuffle partition count for a 12k-row set,
        # and a warehouse co-located with the repo.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.warehouse.dir", str(root / "_warehouse"))
        # Expose the delta root to downstream code. Must be spark.*-prefixed or
        # Spark drops it as a "non-Spark config property".
        .config("spark.gfl.delta.root", str(root))
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def delta_path(layer: str, table: str, spark: SparkSession | None = None) -> str:
    """Resolve a Delta table path, e.g. delta_path('bronze', 'routes').

    Returns <delta_root>/<layer>/<table> as an absolute string path.
    """
    if spark is not None:
        root = Path(spark.conf.get("spark.gfl.delta.root", str(DELTA_ROOT)))
    else:
        root = DELTA_ROOT
    return str(root / layer / table)


if __name__ == "__main__":
    # Smoke test: launch Spark, prove Delta classes load, print a tiny dataset.
    spark = build_spark()
    print("Spark version:", spark.version)
    print("Delta extension:", spark.conf.get("spark.sql.extensions"))
    spark.range(5).show()
    spark.stop()
