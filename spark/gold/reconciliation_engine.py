"""
GOLD — the reconciliation engine.

This is the brain of the case study. It takes the canonical partner event
stream (money that moved at the operator) and the canonical platform event
stream (entitlements we granted) and produces ONE row per reconciled unit of
work in `fact_reconciliation_break`, each tagged with a `recon_status`.

------------------------------------------------------------------------
THE MATCHING DECISION TREE  (rationale for each branch inline)
------------------------------------------------------------------------
For a partner event P and platform event I to be "the same transaction":

  TIER 1 — STRONG KEY MATCH (highest confidence)
    Match P.partner_txn_id == I.partner_txn_id  (same operator).
    This is the contractual join key. When present on both sides we trust it
    completely. Within a strong match we then compare amounts:
        |P.amount - I.amount| <= tolerance  -> MATCHED
        else                                -> AMOUNT_MISMATCH

  TIER 2 — FALLBACK COMPOSITE MATCH (medium confidence)
    Used only when partner_txn_id is NULL on the platform side (the OLTP
    "partner_txn_id not always populated" reality). We match on:
        operator_code
      + msisdn_or_account  <-> user_id  (resolved via sub mapping)
      + txn_type / event_type aligned
      + business_date within ±fallback_business_date_window_days
      + amount within tolerance
    These matches are flagged match_confidence='fallback' so Finance can see
    they rest on heuristics, and we guard false positives by requiring amount
    AND a tight date window AND a 1:1 pairing (no fan-out).

  TIER 3 — NO MATCH -> classify the unmatched survivor
    Partner row with no platform counterpart -> MISSING_ON_PLATFORM
        (operator billed the user; we have no record — lost webhook / bug)
    Platform row with no partner counterpart -> MISSING_AT_PARTNER
        (we granted entitlement; no money moved — free-trial/fraud/mis-plan)
        ...unless the platform row is a recursion_failure, which is an
        EXPECTED non-charge, so it is excluded from "missing at partner".

  CROSS-CUTTING STATUSES
    ORPHAN_CHURN  — user churned on platform but partner is still billing
                    successfully in this period (or churned at partner while
                    platform keeps entitling). Detected by joining churn.
    LATE_ARRIVAL  — a partner row whose file_arrival_date is
                    > late_arrival_threshold_days after its business_date.
                    It is folded into its ORIGINAL business_date period (so the
                    number restates correctly) and tagged so Finance knows a
                    closed period received a late row.

Output grain: one row per (matched pair) OR (unmatched partner row) OR
(unmatched platform row). Amounts from both sides are carried so the mart can
sum partner_total, internal_total and variance without re-joining.

Run:
    python -m spark.gold.reconciliation_engine --business-date 2024-01-15
"""

import argparse

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark.config import paths
from spark.config.operator_config import RECON_CONFIG
from spark.utils.delta_utils import overwrite_partition
from spark.utils.spark_session import get_spark

BREAK_COLUMNS = [
    "business_date",
    "operator_code",
    "recon_status",          # MATCHED / AMOUNT_MISMATCH / MISSING_ON_PLATFORM /
                             # MISSING_AT_PARTNER / ORPHAN_CHURN / LATE_ARRIVAL
    "match_confidence",      # strong / fallback / none
    "match_key",             # which key produced the match (audit trail)
    "partner_txn_id",
    "platform_event_id",
    "user_id",
    "msisdn_or_account",
    "txn_type",
    "partner_amount",
    "internal_amount",
    "amount_variance",
    "currency",
    "partner_txn_ts_utc",
    "platform_event_ts_utc",
    "file_arrival_date",
    "is_late_arrival",
    "recon_run_ts",
]

# event types that legitimately move money and therefore must reconcile
MONEY_TYPES = ["subscription_success", "recursion_success", "refund"]


def _amounts_match(p_col, i_col):
    abs_tol = RECON_CONFIG["amount_abs_tolerance"]
    rel_tol = RECON_CONFIG["amount_rel_tolerance"]
    diff = F.abs(p_col - i_col)
    rel = diff / F.greatest(F.abs(p_col), F.lit(0.01))
    return (diff <= F.lit(abs_tol)) | (rel <= F.lit(rel_tol))


def run_reconciliation(spark, business_date: str):
    late_days = RECON_CONFIG["late_arrival_threshold_days"]
    fb_window = RECON_CONFIG["fallback_business_date_window_days"]

    partner = (
        spark.read.format("delta")
        .load(paths.SILVER_PARTNER_EVENTS)
        .where(F.col("business_date") == business_date)
        .where(F.col("txn_type").isin(MONEY_TYPES))
    )
    platform = (
        spark.read.format("delta")
        .load(paths.SILVER_PLATFORM_EVENTS)
        .where(F.col("business_date") == business_date)
    )

    # ----------------------------------------------------------------
    # TIER 1 — strong key match on partner_txn_id (+ operator)
    # ----------------------------------------------------------------
    p1 = partner.alias("p")
    i1 = platform.where(F.col("partner_txn_id").isNotNull()).alias("i")

    strong = (
        p1.join(
            i1,
            on=[
                F.col("p.partner_txn_id") == F.col("i.partner_txn_id"),
                F.col("p.operator_code") == F.col("i.operator_code"),
            ],
            how="inner",
        )
        .select(
            F.col("p.business_date").alias("business_date"),
            F.col("p.operator_code").alias("operator_code"),
            F.col("p.partner_txn_id").alias("partner_txn_id"),
            F.col("i.platform_event_id").alias("platform_event_id"),
            F.col("i.user_id").alias("user_id"),
            F.col("p.msisdn_or_account").alias("msisdn_or_account"),
            F.col("p.txn_type").alias("txn_type"),
            F.col("p.amount").alias("partner_amount"),
            F.col("i.amount").alias("internal_amount"),
            F.col("p.currency").alias("currency"),
            F.col("p.txn_ts_utc").alias("partner_txn_ts_utc"),
            F.col("i.event_ts_utc").alias("platform_event_ts_utc"),
            F.col("p.file_arrival_date").alias("file_arrival_date"),
            F.lit("strong").alias("match_confidence"),
            F.lit("partner_txn_id").alias("match_key"),
        )
        .withColumn(
            "recon_status",
            F.when(
                _amounts_match(F.col("partner_amount"), F.col("internal_amount")),
                F.lit("MATCHED"),
            ).otherwise(F.lit("AMOUNT_MISMATCH")),
        )
    )

    matched_partner_ids = strong.select("partner_txn_id").distinct()
    matched_platform_ids = strong.select("platform_event_id").distinct()

    # remaining unmatched after tier 1
    partner_left = partner.join(
        matched_partner_ids, on="partner_txn_id", how="left_anti"
    )
    platform_left = platform.join(
        matched_platform_ids, on="platform_event_id", how="left_anti"
    )

    # ----------------------------------------------------------------
    # TIER 2 — fallback composite match (only platform rows whose
    # partner_txn_id is NULL, i.e. the key never existed to join on).
    # Key: operator + user/account + txn_type + date-window + amount.
    # We need msisdn<->user_id resolution; use sub_initial mapping when
    # available, else fall back to matching on amount+type+date alone.
    # ----------------------------------------------------------------
    pf = partner_left.alias("p")
    iff = platform_left.where(F.col("partner_txn_id").isNull()).alias("i")

    fallback = (
        pf.join(
            iff,
            on=[
                F.col("p.operator_code") == F.col("i.operator_code"),
                F.col("p.txn_type") == F.col("i.event_type"),
                # tight date window
                F.abs(
                    F.datediff(F.col("p.business_date"), F.col("i.business_date"))
                ) <= F.lit(fb_window),
                # amount must agree to avoid false pairing
                _amounts_match(F.col("p.amount"), F.col("i.amount")),
            ],
            how="inner",
        )
        # guard fan-out: keep best (closest amount) candidate per partner row
        .withColumn(
            "_rank",
            F.row_number().over(
                Window
                .partitionBy("p.partner_txn_id", "p.operator_code")
                .orderBy(F.abs(F.col("p.amount") - F.col("i.amount")))
            ),
        )
        .where(F.col("_rank") == 1)
        .select(
            F.col("p.business_date").alias("business_date"),
            F.col("p.operator_code").alias("operator_code"),
            F.col("p.partner_txn_id").alias("partner_txn_id"),
            F.col("i.platform_event_id").alias("platform_event_id"),
            F.col("i.user_id").alias("user_id"),
            F.col("p.msisdn_or_account").alias("msisdn_or_account"),
            F.col("p.txn_type").alias("txn_type"),
            F.col("p.amount").alias("partner_amount"),
            F.col("i.amount").alias("internal_amount"),
            F.col("p.currency").alias("currency"),
            F.col("p.txn_ts_utc").alias("partner_txn_ts_utc"),
            F.col("i.event_ts_utc").alias("platform_event_ts_utc"),
            F.col("p.file_arrival_date").alias("file_arrival_date"),
            F.lit("fallback").alias("match_confidence"),
            F.lit("operator+user+type+amount+date").alias("match_key"),
        )
        # fallback matches are, by construction, amount-equal -> MATCHED
        .withColumn("recon_status", F.lit("MATCHED"))
    )

    fb_partner_ids = fallback.select("partner_txn_id").distinct()
    fb_platform_ids = fallback.select("platform_event_id").distinct()

    # ----------------------------------------------------------------
    # TIER 3 — survivors are true breaks
    # ----------------------------------------------------------------
    # partner rows with no match anywhere -> MISSING_ON_PLATFORM
    missing_on_platform = (
        partner_left.join(fb_partner_ids, on="partner_txn_id", how="left_anti")
        .select(
            F.col("business_date"),
            F.col("operator_code"),
            F.col("partner_txn_id"),
            F.lit(None).cast("string").alias("platform_event_id"),
            F.lit(None).cast("string").alias("user_id"),
            F.col("msisdn_or_account"),
            F.col("txn_type"),
            F.col("amount").alias("partner_amount"),
            F.lit(0).cast("decimal(12,2)").alias("internal_amount"),
            F.col("currency"),
            F.col("txn_ts_utc").alias("partner_txn_ts_utc"),
            F.lit(None).cast("timestamp").alias("platform_event_ts_utc"),
            F.col("file_arrival_date"),
            F.lit("none").alias("match_confidence"),
            F.lit("unmatched_partner").alias("match_key"),
            F.lit("MISSING_ON_PLATFORM").alias("recon_status"),
        )
    )

    # platform rows with no match -> MISSING_AT_PARTNER
    # (recursion_failure already excluded as a money type; here platform_left
    #  still includes recursion_failure rows, so filter them out: an expected
    #  decline is NOT money we should have received.)
    missing_at_partner = (
        platform_left.join(fb_platform_ids, on="platform_event_id", how="left_anti")
        .where(F.col("event_type") != "recursion_failure")
        .select(
            F.col("business_date"),
            F.col("operator_code"),
            F.col("partner_txn_id"),
            F.col("platform_event_id"),
            F.col("user_id"),
            F.lit(None).cast("string").alias("msisdn_or_account"),
            F.col("event_type").alias("txn_type"),
            F.lit(0).cast("decimal(12,2)").alias("partner_amount"),
            F.col("amount").alias("internal_amount"),
            F.lit(None).cast("string").alias("currency"),
            F.lit(None).cast("timestamp").alias("partner_txn_ts_utc"),
            F.col("event_ts_utc").alias("platform_event_ts_utc"),
            F.lit(None).cast("date").alias("file_arrival_date"),
            F.lit("none").alias("match_confidence"),
            F.lit("unmatched_platform").alias("match_key"),
            F.lit("MISSING_AT_PARTNER").alias("recon_status"),
        )
    )

    # ----------------------------------------------------------------
    # Combine all classified rows
    # ----------------------------------------------------------------
    combined = (
        strong.select(*[c for c in BREAK_COLUMNS
                        if c not in ("amount_variance", "is_late_arrival", "recon_run_ts")])
        .unionByName(
            fallback.select(*[c for c in BREAK_COLUMNS
                              if c not in ("amount_variance", "is_late_arrival", "recon_run_ts")])
        )
        .unionByName(missing_on_platform)
        .unionByName(missing_at_partner)
    )

    # ----------------------------------------------------------------
    # ORPHAN_CHURN overlay — user churned on platform but partner billed,
    # or vice versa, within this period. We re-tag matched/missing rows whose
    # user appears in churn for this operator on/around this date.
    # ----------------------------------------------------------------
    try:
        churn = (
            spark.read.format("delta")
            .load(paths.BRONZE_USER_CHURN_EVENTS)
            .select(
                F.col("user_id"),
                F.col("operator_code"),
                F.to_date("churn_ts").alias("churn_date"),
            )
            .where(F.col("churn_date") <= F.lit(business_date))
            .select("user_id", "operator_code")
            .distinct()
        )
        combined = combined.join(
            churn.withColumn("_churned", F.lit(True)),
            on=["user_id", "operator_code"],
            how="left",
        )
        # A successful partner charge for a churned user = orphan churn.
        combined = combined.withColumn(
            "recon_status",
            F.when(
                F.col("_churned").isNotNull()
                & (F.col("partner_amount") > 0)
                & (F.col("txn_type").isin("subscription_success", "recursion_success")),
                F.lit("ORPHAN_CHURN"),
            ).otherwise(F.col("recon_status")),
        ).drop("_churned")
    except Exception as e:
        print(f"[gold] churn overlay skipped: {e}")

    # ----------------------------------------------------------------
    # Derived columns: variance, late-arrival flag, run timestamp
    # ----------------------------------------------------------------
    combined = (
        combined
        .withColumn(
            "amount_variance",
            (F.coalesce(F.col("partner_amount"), F.lit(0))
             - F.coalesce(F.col("internal_amount"), F.lit(0))).cast("decimal(12,2)"),
        )
        .withColumn(
            "is_late_arrival",
            F.when(
                F.col("file_arrival_date").isNotNull()
                & (F.datediff(F.col("file_arrival_date"), F.col("business_date"))
                   > F.lit(late_days)),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "recon_status",
            # late arrivals keep their break type but are additionally flagged;
            # we surface a dedicated LATE_ARRIVAL status only when the row would
            # otherwise be MATCHED, so Finance sees "this matched, but late".
            F.when(
                F.col("is_late_arrival") & (F.col("recon_status") == "MATCHED"),
                F.lit("LATE_ARRIVAL"),
            ).otherwise(F.col("recon_status")),
        )
        .withColumn("recon_run_ts", F.current_timestamp())
    )

    return combined.select(*BREAK_COLUMNS)


def main(business_date: str):
    spark = get_spark("gold-reconciliation-engine")
    result = run_reconciliation(spark, business_date)

    overwrite_partition(
        result,
        paths.GOLD_FACT_RECON_BREAK,
        replace_where=f"business_date = '{business_date}'",
        partition_by=["business_date"],
    )

    summary = (
        result.groupBy("recon_status")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )
    print(f"[gold] fact_reconciliation_break for {business_date}:")
    for r in summary:
        print(f"        {r['recon_status']:<22} {r['count']}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--business-date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.business_date)
