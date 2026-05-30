"""
BRONZE — internal OLTP tables (arriving via CDC).

The platform OLTP exposes, PER OPERATOR:
    sub_initial_{operator}
    sub_recursion_success_{operator}
    sub_recursion_failure_{operator}
and one shared:
    user_churn_events

CDC means we receive inserts/updates/deletes. For the case study we treat the
landed CDC as the latest image and MERGE on each table's primary key, so a
re-delivered change updates the row rather than duplicating it.

We deliberately keep the per-operator tables SEPARATE at bronze (one Delta
table per logical table, partitioned by operator_code) and only UNION them in
silver. That mirrors how schema drift between operators is best contained:
isolate at ingest, unify once normalized.

Run:
    python -m spark.ingestion.bronze_internal_cdc --arrival-date 2024-01-15
"""

import argparse
import glob
import os

from pyspark.sql import functions as F

from spark.config import paths
from spark.config.operator_config import OPERATORS
from spark.utils.delta_utils import merge_upsert
from spark.utils.spark_session import get_spark

# logical table -> (bronze path, primary key, landing subfolder prefix)
INTERNAL_TABLES = {
    "sub_initial": {
        "path": paths.BRONZE_SUB_INITIAL,
        "pk": "sub_id",
        "per_operator": True,
    },
    "sub_recursion_success": {
        "path": paths.BRONZE_SUB_RECURSION_SUCCESS,
        "pk": "recursion_id",
        "per_operator": True,
    },
    "sub_recursion_failure": {
        "path": paths.BRONZE_SUB_RECURSION_FAILURE,
        "pk": "failure_id",
        "per_operator": True,
    },
    "user_churn_events": {
        "path": paths.BRONZE_USER_CHURN_EVENTS,
        "pk": "user_id",            # composite in practice; see note in silver
        "per_operator": False,
    },
}


def _read_json(spark, file_pattern: str, arrival_date: str):
    matches = glob.glob(file_pattern)
    matches = [m for m in matches if arrival_date in m]
    if not matches:
        return None
    return spark.read.json(matches)


def ingest_table(spark, logical_name: str, spec: dict, arrival_date: str) -> int:
    total = 0
    if spec["per_operator"]:
        # one physical landing file per operator: sub_initial_telco_a_<date>.json
        for op in OPERATORS:
            physical = f"{logical_name}_{op}"
            pattern = os.path.join(
                paths.LANDING_INTERNAL, physical, f"{physical}_*.json"
            )
            df = _read_json(spark, pattern, arrival_date)
            if df is None:
                continue
            df = (
                df.withColumn("operator_code", F.lit(op))
                .withColumn("_ingested_at", F.current_timestamp())
                .withColumn("file_arrival_date", F.lit(arrival_date).cast("date"))
            )
            merge_upsert(
                spark, df, spec["path"],
                merge_keys=[spec["pk"], "operator_code"],
                partition_by=["operator_code"],
            )
            n = df.count()
            total += n
            print(f"[bronze] {physical}: merged {n} rows")
    else:
        pattern = os.path.join(
            paths.LANDING_INTERNAL, logical_name, f"{logical_name}_*.json"
        )
        df = _read_json(spark, pattern, arrival_date)
        if df is not None:
            df = (
                df.withColumn("_ingested_at", F.current_timestamp())
                .withColumn("file_arrival_date", F.lit(arrival_date).cast("date"))
            )
            # churn key is (user_id, operator_code, churn_ts); use all three
            merge_upsert(
                spark, df, spec["path"],
                merge_keys=["user_id", "operator_code", "churn_ts"],
                partition_by=["operator_code"],
            )
            n = df.count()
            total += n
            print(f"[bronze] {logical_name}: merged {n} rows")
    return total


def main(arrival_date: str):
    spark = get_spark("bronze-internal-cdc")
    for name, spec in INTERNAL_TABLES.items():
        ingest_table(spark, name, spec, arrival_date)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--arrival-date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.arrival_date)
