"""
Register the Delta output paths as tables in the shared Hive Metastore.

The Spark jobs write Delta to plain paths on MinIO (under $LAKEHOUSE_ROOT). The
metastore talks in terms of a *catalog*, not paths, so every layer's Delta
location is registered here as an external table. That makes the whole
lakehouse — bronze, silver and gold — queryable from dbt and from Trino/DBeaver
against the same catalog.

On Databricks the equivalent tables already live in Unity Catalog (the Spark
jobs would saveAsTable there), so this is a no-op step in that environment.

Run:
    python spark/register_tables.py
"""

from spark.config import paths
from spark.utils.delta_utils import table_exists
from spark.utils.spark_session import get_spark

# (database, table) -> delta path
TABLES = {
    ("bronze", "operator_feeds"): paths.BRONZE_OPERATOR_FEEDS,
    ("bronze", "sub_initial"): paths.BRONZE_SUB_INITIAL,
    ("bronze", "sub_recursion_success"): paths.BRONZE_SUB_RECURSION_SUCCESS,
    ("bronze", "sub_recursion_failure"): paths.BRONZE_SUB_RECURSION_FAILURE,
    ("bronze", "user_churn_events"): paths.BRONZE_USER_CHURN_EVENTS,
    ("silver", "partner_events"): paths.SILVER_PARTNER_EVENTS,
    ("silver", "platform_events"): paths.SILVER_PLATFORM_EVENTS,
    ("gold", "fact_reconciliation_break"): paths.GOLD_FACT_RECON_BREAK,
}


def main():
    spark = get_spark("register-tables", use_hive=True)
    for (db, table), path in TABLES.items():
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        # Skip a table whose Delta path doesn't exist yet (e.g. no recursion
        # failures landed that day) so registration never fails on a missing path.
        if not table_exists(spark, path):
            print(f"[register] skip {db}.{table} (no data at {path})")
            continue
        # DROP + CREATE (not CREATE IF NOT EXISTS): a stale registration from an
        # earlier run can point at an old location / empty schema, and IF NOT
        # EXISTS would keep it. Dropping an EXTERNAL Delta table removes only the
        # catalog entry, never the data, so re-registering is always safe.
        spark.sql(f"DROP TABLE IF EXISTS {db}.{table}")
        spark.sql(f"CREATE TABLE {db}.{table} USING DELTA LOCATION '{path}'")
        print(f"[register] {db}.{table} -> {path}")


if __name__ == "__main__":
    main()
