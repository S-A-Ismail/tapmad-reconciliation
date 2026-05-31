"""
SILVER — platform (internal) events, normalized to the canonical schema.

Turns the per-operator OLTP tables into ONE canonical platform event stream
that lines up 1:1 with the partner event stream, so the reconciliation engine
can compare like-for-like.

The "how do you join across operator-suffixed tables" requirement is answered
here. Bronze already unified each logical table across operators (via the
operator_code column we stamped at ingest), so silver just:
  * UNIONs the three transaction-bearing tables into one event stream
  * maps each into the canonical event shape (event_type, amount, ts, keys)
  * converts internal timestamps (assumed platform-UTC) to business_date
  * pulls churn into its own canonical stream used for orphan-churn detection

Canonical platform event schema:
    operator_code, platform_event_id, event_type, sub_id, user_id,
    partner_txn_id, plan_id, amount, event_ts_utc, business_date

event_type values align with partner txn_type where they overlap:
    subscription_success, recursion_success, recursion_failure

Run:
    python -m spark.silver.silver_internal_events --business-date 2024-01-15
"""

import argparse

from pyspark.sql import functions as F

from spark.config import paths
from spark.utils.delta_utils import overwrite_partition
from spark.utils.spark_session import get_spark

PLATFORM_COLUMNS = [
    "operator_code",
    "platform_event_id",
    "event_type",
    "sub_id",
    "user_id",
    "partner_txn_id",
    "plan_id",
    "amount",
    "event_ts_utc",
    "business_date",
]


def _load(spark, path):
    try:
        return spark.read.format("delta").load(path)
    except Exception:
        return None


def build_platform_events(spark, business_date: str):
    frames = []

    # --- sub_initial -> subscription_success events ---------------------
    si = _load(spark, paths.BRONZE_SUB_INITIAL)
    if si is not None:
        frames.append(
            si.select(
                F.col("operator_code"),
                F.col("sub_id").alias("platform_event_id"),
                F.lit("subscription_success").alias("event_type"),
                F.col("sub_id"),
                F.col("user_id"),
                F.col("partner_txn_id"),
                F.col("plan_id"),
                F.col("amount").cast("decimal(12,2)").alias("amount"),
                F.to_timestamp("created_ts").alias("event_ts_utc"),
            )
            # Only "active"/"pending" subs represent a real charge attempt we
            # expect the partner to have billed. 'failed' internal rows are not
            # money-moving and would create false "missing at partner" breaks.
            .where(F.col("status").isin("active", "pending"))
        )

    # --- sub_recursion_success -> recursion_success events --------------
    rs = _load(spark, paths.BRONZE_SUB_RECURSION_SUCCESS)
    if rs is not None:
        frames.append(
            rs.select(
                F.col("operator_code"),
                F.col("recursion_id").alias("platform_event_id"),
                F.lit("recursion_success").alias("event_type"),
                F.col("sub_id"),
                F.col("user_id"),
                F.col("partner_txn_id"),
                F.lit(None).cast("string").alias("plan_id"),
                F.col("amount").cast("decimal(12,2)").alias("amount"),
                F.to_timestamp("recurrence_ts").alias("event_ts_utc"),
            )
        )

    # --- sub_recursion_failure -> recursion_failure events --------------
    # Failures move no money, but we keep them so the engine can explain a
    # "missing at partner" as an expected decline rather than a true break.
    rf = _load(spark, paths.BRONZE_SUB_RECURSION_FAILURE)
    if rf is not None:
        frames.append(
            rf.select(
                F.col("operator_code"),
                F.col("failure_id").alias("platform_event_id"),
                F.lit("recursion_failure").alias("event_type"),
                F.col("sub_id"),
                F.col("user_id"),
                F.lit(None).cast("string").alias("partner_txn_id"),
                F.lit(None).cast("string").alias("plan_id"),
                F.lit(0).cast("decimal(12,2)").alias("amount"),
                F.to_timestamp("attempt_ts").alias("event_ts_utc"),
            )
        )

    if not frames:
        return None

    events = frames[0]
    for f in frames[1:]:
        events = events.unionByName(f)

    # internal timestamps are platform-UTC already -> business_date directly
    events = events.withColumn("business_date", F.to_date("event_ts_utc"))
    events = events.where(F.col("business_date") == F.lit(business_date))
    return events.select(*PLATFORM_COLUMNS)


def main(business_date: str):
    spark = get_spark("silver-platform-events")
    events = build_platform_events(spark, business_date)
    if events is None:
        print(f"[silver] platform_events: nothing for {business_date}")
        return

    overwrite_partition(
        events,
        paths.SILVER_PLATFORM_EVENTS,
        replace_where=f"business_date = '{business_date}'",
        partition_by=["business_date"],
    )
    print(f"[silver] platform_events: wrote {events.count()} rows for {business_date}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--business-date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.business_date)
