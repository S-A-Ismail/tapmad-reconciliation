#!/usr/bin/env bash
# Container entrypoint for the batch pipeline: generate -> ingest -> normalize
# -> reconcile, for one business date. The gold step prints the per-operator
# reconciliation summary at the end.
#
# Override the date / row count with env vars:
#   docker compose run --rm -e BUSINESS_DATE=2024-02-01 -e N=500 pipeline
set -euo pipefail

BUSINESS_DATE="${BUSINESS_DATE:-2024-01-15}"
N="${N:-300}"

echo ">> generate synthetic landing data for ${BUSINESS_DATE} (n=${N})"
python data/synthetic/generate_data.py --business-date "${BUSINESS_DATE}" --n "${N}"

echo ">> bronze"
python -m spark.ingestion.bronze_operator_feeds --arrival-date "${BUSINESS_DATE}"
python -m spark.ingestion.bronze_internal_cdc   --arrival-date "${BUSINESS_DATE}"

echo ">> silver"
python -m spark.silver.silver_operator_feeds  --business-date "${BUSINESS_DATE}"
python -m spark.silver.silver_internal_events --business-date "${BUSINESS_DATE}"

echo ">> gold (prints the reconciliation summary)"
python -m spark.gold.reconciliation_engine --business-date "${BUSINESS_DATE}"
