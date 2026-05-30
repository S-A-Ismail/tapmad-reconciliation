"""
BRONZE — operator feeds.

Reads whatever the 6-7 operators dropped into the landing zone for a given
file_arrival_date and lands it in Delta with ingestion metadata attached.

Design notes:
  * Schema-on-read per operator. We do NOT force a shared schema here. Bronze
    keeps each operator's raw columns (prefixed) plus a normalized core, so we
    never lose source fidelity for audit/debugging.
  * One generic job driven by OPERATOR_CONFIG, not 7 copies.
  * MERGE on (operator_code, partner_txn_id, file_arrival_date) so that when an
    operator re-sends the last 3 days as corrections, rows update in place.
    Bronze is therefore exactly-once at the natural-key grain.

Run:
    python -m spark.ingestion.bronze_operator_feeds --arrival-date 2024-01-15
"""

import argparse
import glob
import os

from pyspark.sql import functions as F

from spark.config import paths
from spark.config.operator_config import OPERATOR_CONFIG
from spark.utils.delta_utils import merge_upsert
from spark.utils.spark_session import get_spark


def _read_raw(spark, operator_code: str, cfg: dict, arrival_date: str):
    """Read one operator's files for one arrival date as raw strings.

    We read everything as string at bronze. Type-casting belongs in silver,
    where we can quarantine bad values instead of failing the whole file.
    """
    pattern = os.path.join(
        paths.LANDING_OPERATOR, operator_code, cfg["file_glob"]
    )
    matches = glob.glob(pattern)
    # In production the path is partitioned by arrival date; locally we filter
    # by the date token embedded in the filename.
    matches = [m for m in matches if arrival_date.replace("-", "") in m
               or arrival_date in m]
    if not matches:
        return None

    fmt = cfg["file_format"]
    if fmt == "csv":
        reader = spark.read
        for k, v in cfg.get("csv_options", {}).items():
            reader = reader.option(k, v)
        # inferSchema=false -> everything string, safest for bronze.
        df = reader.option("inferSchema", "false").csv(matches)
    elif fmt == "json":
        df = spark.read.json(matches)
        # cast every column to string for a uniform bronze contract
        df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])
    else:
        raise ValueError(f"Unsupported file_format {fmt} for {operator_code}")

    return df


def ingest_operator(spark, operator_code: str, arrival_date: str) -> int:
    cfg = OPERATOR_CONFIG[operator_code]
    raw = _read_raw(spark, operator_code, cfg, arrival_date)
    if raw is None:
        print(f"[bronze] {operator_code}: no files for {arrival_date}")
        return 0

    # Pull the canonical key columns out by their RAW names so MERGE has a
    # stable key even though we keep all raw columns too.
    reverse_map = {v: k for k, v in cfg["column_map"].items()}
    raw_txn_id_col = reverse_map["partner_txn_id"]

    bronze = (
        raw
        .withColumn("operator_code", F.lit(operator_code))
        .withColumn("partner_txn_id", F.col(raw_txn_id_col).cast("string"))
        .withColumn("file_arrival_date", F.lit(arrival_date).cast("date"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_format", F.lit(cfg["file_format"]))
    )

    merge_upsert(
        spark,
        bronze,
        paths.BRONZE_OPERATOR_FEEDS,
        merge_keys=["operator_code", "partner_txn_id", "file_arrival_date"],
        partition_by=["operator_code", "file_arrival_date"],
    )
    n = bronze.count()
    print(f"[bronze] {operator_code}: merged {n} rows for {arrival_date}")
    return n


def main(arrival_date: str):
    spark = get_spark("bronze-operator-feeds")
    total = 0
    for op in OPERATOR_CONFIG:
        total += ingest_operator(spark, op, arrival_date)
    print(f"[bronze] operator feeds: {total} total rows for {arrival_date}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--arrival-date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.arrival_date)
