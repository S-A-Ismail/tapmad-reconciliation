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

_LOCAL_DEFAULT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "lakehouse")
)

# Data layers (bronze/silver/gold). Local folder by default, but can be an
# object-store URI such as s3a://bucket/... (Spark reads/writes these).
LAKEHOUSE_ROOT = os.environ.get("LAKEHOUSE_ROOT", _LOCAL_DEFAULT)

# Landing is written by the pure-Python generator via open(), so it MUST be a
# local path even when LAKEHOUSE_ROOT points at object storage. When the lake is
# local we keep landing under it; when it's a URI we fall back to a local folder.
# Override explicitly with LANDING_ROOT (the containers set it to /lakehouse/landing).
_default_landing = (
    os.path.join(_LOCAL_DEFAULT, "landing")
    if "://" in LAKEHOUSE_ROOT
    else os.path.join(LAKEHOUSE_ROOT, "landing")
)
LANDING_ROOT = os.environ.get("LANDING_ROOT", _default_landing)


def _join(root: str, *parts: str) -> str:
    """Forward-slash join. os.path.join would use backslashes on Windows and
    corrupt s3a:// URIs; '/' is correct for object stores and accepted by
    Spark / local FS alike."""
    return "/".join([root.rstrip("/"), *parts])


# Landing zone (input files). Mirrors the SFTP/blob drop in production.
LANDING_OPERATOR = _join(LANDING_ROOT, "operator_feeds")
LANDING_INTERNAL = _join(LANDING_ROOT, "internal")

# Bronze (raw + metadata).
BRONZE_OPERATOR_FEEDS = _join(LAKEHOUSE_ROOT, "bronze", "operator_feeds")
BRONZE_SUB_INITIAL = _join(LAKEHOUSE_ROOT, "bronze", "sub_initial")
BRONZE_SUB_RECURSION_SUCCESS = _join(LAKEHOUSE_ROOT, "bronze", "sub_recursion_success")
BRONZE_SUB_RECURSION_FAILURE = _join(LAKEHOUSE_ROOT, "bronze", "sub_recursion_failure")
BRONZE_USER_CHURN_EVENTS = _join(LAKEHOUSE_ROOT, "bronze", "user_churn_events")

# Silver (canonical).
SILVER_PARTNER_EVENTS = _join(LAKEHOUSE_ROOT, "silver", "partner_events")
SILVER_PLATFORM_EVENTS = _join(LAKEHOUSE_ROOT, "silver", "platform_events")

# Gold (reconciliation + marts).
GOLD_FACT_RECON_BREAK = _join(LAKEHOUSE_ROOT, "gold", "fact_reconciliation_break")
GOLD_RECON_DAILY = _join(LAKEHOUSE_ROOT, "gold", "reconciliation_daily")


def ensure_dirs():
    """Create landing dirs locally so first-run doesn't fail on missing path."""
    for p in (LANDING_OPERATOR, LANDING_INTERNAL):
        os.makedirs(p, exist_ok=True)
