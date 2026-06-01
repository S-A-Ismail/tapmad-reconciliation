"""
One-shot local pipeline runner (no Airflow required).

Runs the full medallion flow for one business date on a local Spark, so you can
demo the whole thing end-to-end on a laptop:

    python run_local.py --business-date 2024-01-15 --n 300

It executes, in order:
    1. generate synthetic landing data
    2. bronze operator feeds + bronze internal CDC
    3. silver partner events + silver platform events
    4. gold reconciliation engine
    5. preview reconciliation_daily-style summary (Spark, no dbt needed)

The dbt marts (reconciliation_daily, revenue_monthly_close) are built
separately with `dbt build` against your Databricks warehouse — see README.
This script previews the same aggregation in Spark so you can eyeball results
without a warehouse connection.
"""

import argparse
import subprocess
import sys

from pyspark.sql import functions as F

from spark.config import paths
from spark.utils.spark_session import get_spark


def sh(module_or_script, flag, value, is_script=False):
    cmd = [sys.executable]
    cmd += [module_or_script] if is_script else ["-m", module_or_script]
    cmd += [flag, value]
    print(f"\n=== running: {' '.join(cmd)} ===")
    subprocess.run(cmd, check=True)


def preview(business_date: str):
    spark = get_spark("preview")
    print("\n================ reconciliation_daily (preview) ================")
    fact = (
        spark.read.format("delta")
        .load(paths.GOLD_FACT_RECON_BREAK)
        .where(F.col("business_date") == business_date)
    )
    (
        fact.groupBy("operator_code")
        .agg(
            F.sum(F.when(F.col("recon_status") == "MATCHED", 1).otherwise(0)).alias("matched"),
            F.sum(F.when(F.col("recon_status") == "AMOUNT_MISMATCH", 1).otherwise(0)).alias("amt_mismatch"),
            F.sum(F.when(F.col("recon_status") == "MISSING_ON_PLATFORM", 1).otherwise(0)).alias("missing_platform"),
            F.sum(F.when(F.col("recon_status") == "MISSING_AT_PARTNER", 1).otherwise(0)).alias("missing_partner"),
            F.sum(F.when(F.col("recon_status") == "ORPHAN_CHURN", 1).otherwise(0)).alias("orphan_churn"),
            F.sum(F.when(F.col("recon_status") == "LATE_ARRIVAL", 1).otherwise(0)).alias("late_arrival"),
            F.round(F.sum("partner_amount"), 2).alias("partner_total"),
            F.round(F.sum("internal_amount"), 2).alias("internal_total"),
            F.round(F.sum("amount_variance"), 2).alias("variance"),
        )
        .orderBy("operator_code")
        .show(truncate=False)
    )


def main(business_date: str, n: int):
    sh("data/synthetic/generate_data.py", "--business-date", business_date, is_script=True)
    # generate_data uses --n too; re-run with n if non-default
    if n != 200:
        subprocess.run(
            [sys.executable, "data/synthetic/generate_data.py",
             "--business-date", business_date, "--n", str(n)],
            check=True,
        )

    sh("spark.ingestion.bronze_operator_feeds", "--arrival-date", business_date)
    sh("spark.ingestion.bronze_internal_cdc", "--arrival-date", business_date)
    sh("spark.silver.silver_operator_feeds", "--business-date", business_date)
    sh("spark.silver.silver_internal_events", "--business-date", business_date)
    sh("spark.gold.reconciliation_engine", "--business-date", business_date)
    preview(business_date)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--business-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--n", type=int, default=300)
    args = ap.parse_args()
    main(args.business_date, args.n)
