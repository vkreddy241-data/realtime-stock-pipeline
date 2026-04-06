"""
Delta Lake maintenance job:
  - OPTIMIZE (bin-packing) to merge small files
  - VACUUM to remove stale files older than retention window
  - Z-ORDER by ticker for faster predicate pushdown
Run daily via Airflow.
"""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)

DELTA_PATHS = {
    "raw_ticks": "s3a://vkreddy-stock-lake/delta/stock_ticks/raw",
    "vwap_1m":   "s3a://vkreddy-stock-lake/delta/stock_ticks/vwap_1m",
}
VACUUM_RETAIN_HOURS = 168  # 7 days


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DeltaOptimizer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )


def optimize_table(spark: SparkSession, path: str, zorder_col: str = "ticker"):
    logger.info(f"Optimizing {path} ...")
    dt = DeltaTable.forPath(spark, path)
    dt.optimize().executeZOrderBy(zorder_col)
    logger.info(f"Vacuuming {path} (retain {VACUUM_RETAIN_HOURS}h) ...")
    dt.vacuum(VACUUM_RETAIN_HOURS)
    history = dt.history(5).select("version", "timestamp", "operation", "operationMetrics")
    history.show(truncate=False)


def main():
    spark = build_spark()
    for name, path in DELTA_PATHS.items():
        try:
            optimize_table(spark, path)
        except Exception as e:
            logger.error(f"Failed to optimize {name}: {e}")
    spark.stop()


if __name__ == "__main__":
    main()
