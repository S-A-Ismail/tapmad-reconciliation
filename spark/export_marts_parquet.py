"""
Export the dbt marts as plain Parquet, alongside the bronze/silver/gold exports.

Run this AFTER `dbt build` (the dbt service does). It reads the mart tables dbt
materialized into the local Hive metastore and writes single-file Parquet
snapshots to $LAKEHOUSE_ROOT/exports/marts/<table>/.

Only meaningful for the local dbt-spark target (the marts live in the in-container
metastore). For the Databricks target the tables aren't in local Spark, so this
skips them gracefully.

Run:
    python -m spark.export_marts_parquet
"""

import os

from spark.config import paths
from spark.utils.spark_session import get_spark

# dbt builds marts into "<profile_schema>_<custom_schema>" = "recon_marts" by
# default (profile schema "recon" + models marts +schema "marts"). Override with
# DBT_MARTS_SCHEMA if you change the dbt schema config.
MARTS_SCHEMA = os.environ.get("DBT_MARTS_SCHEMA", "recon_marts")
MARTS = ["reconciliation_daily", "revenue_monthly_close"]

EXPORT_ROOT = os.path.join(paths.LAKEHOUSE_ROOT, "exports", "marts")


def main():
    spark = get_spark("export-marts-parquet")
    for table in MARTS:
        fq = f"{MARTS_SCHEMA}.{table}"
        try:
            df = spark.read.table(fq)
        except Exception as e:  # table not present (e.g. databricks target)
            print(f"[export-marts] skip {fq}: {type(e).__name__}")
            continue
        out = os.path.join(EXPORT_ROOT, table)
        df.coalesce(1).write.mode("overwrite").parquet(out)
        print(f"[export-marts] {fq} -> {out}")
    print(f"[export-marts] parquet snapshots written under {EXPORT_ROOT}")


if __name__ == "__main__":
    main()
