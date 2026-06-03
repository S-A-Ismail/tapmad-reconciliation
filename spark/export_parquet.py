"""
Export the Delta tables as plain Parquet snapshots.

The lakehouse stores everything as Delta (parquet part-files + a _delta_log).
This writes a clean single-file Parquet copy of each bronze / silver / gold
table under $LAKEHOUSE_ROOT/exports/<layer>/<table>/. With the bind-mounted
lakehouse, those land directly on your machine (./lakehouse/exports/...).

Run:
    python -m spark.export_parquet
"""

import os

from spark.config import paths
from spark.utils.delta_utils import table_exists
from spark.utils.spark_session import get_spark

# layer -> { table_name: delta_path }
TABLES = {
    "bronze": {
        "operator_feeds": paths.BRONZE_OPERATOR_FEEDS,
        "sub_initial": paths.BRONZE_SUB_INITIAL,
        "sub_recursion_success": paths.BRONZE_SUB_RECURSION_SUCCESS,
        "sub_recursion_failure": paths.BRONZE_SUB_RECURSION_FAILURE,
        "user_churn_events": paths.BRONZE_USER_CHURN_EVENTS,
    },
    "silver": {
        "partner_events": paths.SILVER_PARTNER_EVENTS,
        "platform_events": paths.SILVER_PLATFORM_EVENTS,
    },
    "gold": {
        "fact_reconciliation_break": paths.GOLD_FACT_RECON_BREAK,
    },
}

EXPORT_ROOT = os.path.join(paths.LAKEHOUSE_ROOT, "exports")


def main():
    spark = get_spark("export-parquet")
    for layer, tables in TABLES.items():
        for name, path in tables.items():
            if not table_exists(spark, path):
                print(f"[export] skip {layer}.{name} (no data yet)")
                continue
            out = os.path.join(EXPORT_ROOT, layer, name)
            # coalesce(1): one part-*.parquet per table (data is small) so the
            # files are easy to open locally.
            (
                spark.read.format("delta").load(path)
                .coalesce(1)
                .write.mode("overwrite").parquet(out)
            )
            print(f"[export] {layer}.{name} -> {out}")
    print(f"[export] parquet snapshots written under {EXPORT_ROOT}")


if __name__ == "__main__":
    main()
