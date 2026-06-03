"""
Centralized storage paths (the "medallion" layout).

Locally these are folders under ./lakehouse. On Databricks they become
ADLS / Unity Catalog locations (see the README's "Migration to Azure"
section). Keeping them in one module means the migration touches exactly
one file.

Layout:
    landing/   raw files exactly as operators / CDC drop them
    bronze/    raw rows + ingestion metadata, schema-on-read, deduped by key
    silver/    canonical, normalized, UTC, one row per business event
    gold/      reconciliation results + Finance-facing marts
"""

import os

# Root of the local lakehouse. Override with env var in CI / other machines.
LAKEHOUSE_ROOT = os.environ.get(
    "LAKEHOUSE_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "lakehouse")),
)

# Landing zone (input files). Mirrors the SFTP/blob drop in production.
LANDING_OPERATOR = os.path.join(LAKEHOUSE_ROOT, "landing", "operator_feeds")
LANDING_INTERNAL = os.path.join(LAKEHOUSE_ROOT, "landing", "internal")

# Bronze (raw + metadata).
BRONZE_OPERATOR_FEEDS = os.path.join(LAKEHOUSE_ROOT, "bronze", "operator_feeds")
BRONZE_SUB_INITIAL = os.path.join(LAKEHOUSE_ROOT, "bronze", "sub_initial")
BRONZE_SUB_RECURSION_SUCCESS = os.path.join(LAKEHOUSE_ROOT, "bronze", "sub_recursion_success")
BRONZE_SUB_RECURSION_FAILURE = os.path.join(LAKEHOUSE_ROOT, "bronze", "sub_recursion_failure")
BRONZE_USER_CHURN_EVENTS = os.path.join(LAKEHOUSE_ROOT, "bronze", "user_churn_events")

# Silver (canonical).
SILVER_PARTNER_EVENTS = os.path.join(LAKEHOUSE_ROOT, "silver", "partner_events")
SILVER_PLATFORM_EVENTS = os.path.join(LAKEHOUSE_ROOT, "silver", "platform_events")

# Gold (reconciliation + marts).
GOLD_FACT_RECON_BREAK = os.path.join(LAKEHOUSE_ROOT, "gold", "fact_reconciliation_break")
GOLD_RECON_DAILY = os.path.join(LAKEHOUSE_ROOT, "gold", "reconciliation_daily")


def ensure_dirs():
    """Create landing dirs locally so first-run doesn't fail on missing path."""
    for p in (LANDING_OPERATOR, LANDING_INTERNAL):
        os.makedirs(p, exist_ok=True)
