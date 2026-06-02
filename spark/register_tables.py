"""
Register the Delta output paths as tables in the local Hive metastore.

The Spark jobs write Delta to plain paths under $LAKEHOUSE_ROOT. dbt's
spark-session target talks to a *catalog*, not paths, so before dbt can read
silver/gold it needs those paths registered as external tables. This script
does that idempotently (CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION).

Local container path only. On Databricks the equivalent tables already live in
Unity Catalog (the Spark jobs would saveAsTable there), so this is a no-op step
in that environment.

Run:
    python spark/register_tables.py
"""

from spark.config import paths
from spark.utils.spark_session import get_spark

# (database, table) -> delta path
TABLES = {
    ("silver", "partner_events"): paths.SILVER_PARTNER_EVENTS,
    ("silver", "platform_events"): paths.SILVER_PLATFORM_EVENTS,
    ("gold", "fact_reconciliation_break"): paths.GOLD_FACT_RECON_BREAK,
}


def main():
    spark = get_spark("register-tables")
    for (db, table), path in TABLES.items():
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        # external Delta table over the path the pipeline already wrote
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {db}.{table} USING DELTA LOCATION '{path}'"
        )
        print(f"[register] {db}.{table} -> {path}")


if __name__ == "__main__":
    main()
