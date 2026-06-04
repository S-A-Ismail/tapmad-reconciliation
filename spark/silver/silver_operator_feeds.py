"""
SILVER — partner (operator) events, normalized to the canonical schema.

This is where the "6-7 differently-shaped feeds become ONE schema" requirement
is satisfied. For each operator we:
  1. rename raw columns -> canonical columns via OPERATOR_CONFIG.column_map
  2. map raw txn_type values -> canonical txn_type
  3. parse the operator's timestamp (incl. epoch-millis & ISO-with-offset)
  4. convert operator-local time -> UTC (txn_ts_utc) keeping the local copy
  5. derive business_date from UTC time (the reconciliation bucket)
  6. cast amount to decimal(12,2)

Output: silver.partner_events — one canonical row per partner transaction,
partitioned by business_date for cheap day-level recompute.

Idempotency: we recompute from bronze for the requested business_date(s) and
`replaceWhere` exactly those partitions. Re-running is deterministic.

Run:
    python -m spark.silver.silver_operator_feeds --business-date 2024-01-15
"""

import argparse

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark.config import paths
from spark.config.operator_config import (
    CANONICAL_TXN_TYPES,
    OPERATOR_CONFIG,
)
from spark.utils.delta_utils import overwrite_partition
from spark.utils.spark_session import get_spark

CANONICAL_COLUMNS = [
    "operator_code",
    "partner_txn_id",
    "msisdn_or_account",
    "txn_type",
    "plan_code",
    "amount",
    "currency",
    "txn_ts_utc",
    "txn_ts_local",
    "business_date",
    "file_arrival_date",
]


def _parse_timestamp(df, cfg):
    """Return df with `txn_ts_local` as a proper timestamp in operator-local tz.

    Handles three timestamp encodings seen across operators:
      - epoch_millis  : integer ms since epoch (already absolute/UTC instant)
      - ISO w/ offset : offset carries the zone; parsed directly to instant
      - naive string  : no zone, must be interpreted in the operator's tz
    """
    fmt = cfg["ts_format"]
    raw = F.col("txn_ts_local")

    if fmt == "epoch_millis":
        # epoch is an absolute instant; from_unixtime gives UTC wall-clock.
        ts_instant = (raw.cast("long") / F.lit(1000)).cast("timestamp")
        # mark that this is already an absolute instant (no further tz convert)
        return df.withColumn("_ts_is_instant", F.lit(True)) \
                 .withColumn("txn_ts_parsed", ts_instant)

    if "XXX" in fmt or fmt.endswith("'Z'"):
        # offset / Z present -> Spark parses to an absolute instant.
        ts_instant = F.to_timestamp(raw, fmt)
        return df.withColumn("_ts_is_instant", F.lit(True)) \
                 .withColumn("txn_ts_parsed", ts_instant)

    # naive local time -> parse as wall-clock, tz applied later.
    ts_local = F.to_timestamp(raw, fmt)
    return df.withColumn("_ts_is_instant", F.lit(False)) \
             .withColumn("txn_ts_parsed", ts_local)


def normalize_operator(spark, operator_code: str, business_date: str):
    cfg = OPERATOR_CONFIG[operator_code]
    tz = cfg["timezone"]

    bronze = (
        spark.read.format("delta")
        .load(paths.BRONZE_OPERATOR_FEEDS)
        .where(F.col("operator_code") == operator_code)
    )
    if bronze.rdd.isEmpty():
        return None

    # 1. project ONLY this operator's mapped columns to their canonical names.
    #    Bronze is a single Delta table holding the MERGED superschema of every
    #    operator, so an in-place rename collides: telco_b's "currency_code" ->
    #    "currency" clashes with telco_d's raw "currency" column that also lives
    #    in the merged schema. Selecting via each operator's own column_map drops
    #    all foreign columns and avoids the AMBIGUOUS_REFERENCE entirely.
    select_exprs = [
        F.col(raw_col).alias(canon_col)
        for raw_col, canon_col in cfg["column_map"].items()
    ]
    # operator_code + file_arrival_date are stamped on at bronze ingest (not part
    # of column_map), so carry them through explicitly.
    select_exprs += [F.col("operator_code"), F.col("file_arrival_date")]
    df = bronze.select(*select_exprs)

    # 2. parse timestamp into txn_ts_parsed (+ _ts_is_instant flag)
    df = _parse_timestamp(df, cfg)

    # 3. derive UTC + keep local
    #    - if already an instant: UTC = parsed; local = parsed shown in tz
    #    - if naive local: it's wall-clock in `tz`; to_utc_timestamp converts
    df = df.withColumn(
        "txn_ts_utc",
        F.when(
            F.col("_ts_is_instant"),
            F.col("txn_ts_parsed"),
        ).otherwise(
            F.to_utc_timestamp(F.col("txn_ts_parsed"), tz)
        ),
    ).withColumn(
        "txn_ts_local",
        F.when(
            F.col("_ts_is_instant"),
            F.from_utc_timestamp(F.col("txn_ts_parsed"), tz),
        ).otherwise(
            F.col("txn_ts_parsed")
        ),
    )

    # 4. business_date from UTC instant
    df = df.withColumn("business_date", F.to_date(F.col("txn_ts_utc")))

    # 5. canonical txn_type via mapping; unmapped -> 'unknown'
    mapping = cfg["txn_type_map"]
    map_expr = F.create_map(
        *[x for kv in mapping.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )
    df = df.withColumn(
        "txn_type",
        F.coalesce(map_expr[F.col("txn_type")], F.lit("unknown")),
    )

    # 6. amount + currency tidy-up
    df = df.withColumn("amount", F.col("amount").cast("decimal(12,2)"))
    df = df.withColumn(
        "currency",
        F.coalesce(F.upper(F.col("currency")), F.lit(cfg["default_currency"])),
    )

    return df.select(*CANONICAL_COLUMNS)


def main(business_date: str):
    spark = get_spark("silver-partner-events")
    frames = []
    for op in OPERATOR_CONFIG:
        df = normalize_operator(spark, op, business_date)
        if df is not None:
            frames.append(df)

    if not frames:
        print(f"[silver] partner_events: nothing to do for {business_date}")
        return

    union = frames[0]
    for f in frames[1:]:
        union = union.unionByName(f)

    # Only persist the requested business_date partition (idempotent replace).
    # Note: late-arriving rows can carry an earlier business_date; the daily
    # job is normally run for "today", and the backfill DAG re-runs older dates
    # explicitly. Here we filter to the target date for a clean partition swap.
    union = union.where(F.col("business_date") == F.lit(business_date))

    # Collapse re-send corrections: a transaction re-delivered on a later day
    # carries the SAME partner_txn_id under a newer file_arrival_date. Keep only
    # the latest arrival per (operator_code, partner_txn_id) so a correction
    # supersedes the original instead of double-counting it downstream.
    _latest = Window.partitionBy("operator_code", "partner_txn_id").orderBy(
        F.col("file_arrival_date").desc_nulls_last()
    )
    union = (
        union.withColumn("_rn", F.row_number().over(_latest))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    # data quality flag surfaced downstream, never silently dropped
    unknown = union.where(F.col("txn_type") == "unknown").count()
    if unknown:
        print(f"[silver][DQ] {unknown} partner rows with unmapped txn_type")

    overwrite_partition(
        union,
        paths.SILVER_PARTNER_EVENTS,
        replace_where=f"business_date = '{business_date}'",
        partition_by=["business_date"],
    )
    print(f"[silver] partner_events: wrote {union.count()} rows for {business_date}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--business-date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.business_date)
