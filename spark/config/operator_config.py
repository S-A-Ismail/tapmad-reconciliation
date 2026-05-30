"""
Operator configuration registry.
========================================================================

This is the single source of truth for how each of the 6-7 operator feeds
maps onto our canonical schema. Every operator delivers files in a slightly
different shape (different column names, formats, timezones, currencies),
so instead of writing 7 bespoke ingestion jobs we drive ONE generic job
from this config.

Why a config-driven approach?
  - Onboarding a new operator = adding one dict entry, no new code.
  - The "shape difference" between operators lives as data, not as
    scattered if/else branches.
  - The same config feeds both the Spark normalization job and our docs.

Canonical target schema (what every feed is mapped INTO):
    operator_code      string     e.g. telco_a
    partner_txn_id     string     operator's unique transaction id
    msisdn_or_account  string     operator-side user identifier
    txn_type           string     canonical: subscription_success,
                                   recursion_success, recursion_failure,
                                   refund, cancel
    plan_code          string     operator-side plan reference
    amount             decimal    transaction value in local currency
    currency           string     ISO currency code
    txn_ts_utc         timestamp  txn time converted to UTC
    txn_ts_local       timestamp  original operator-local time (audit)
    business_date      date       date used for reconciliation bucketing
    file_arrival_date  date       date the file physically landed
"""

# Canonical transaction types. Anything an operator sends that does NOT map
# cleanly into one of these is routed to 'unknown' and surfaced as a data
# quality issue rather than being silently dropped.
CANONICAL_TXN_TYPES = {
    "subscription_success",
    "recursion_success",
    "recursion_failure",
    "refund",
    "cancel",
    "unknown",
}


OPERATOR_CONFIG = {
    # ---- Telco A: classic MNO, CSV, local Pakistan time -----------------
    "telco_a": {
        "file_format": "csv",
        "file_glob": "telco_a_*.csv",
        "timezone": "Asia/Karachi",          # UTC+5, no DST
        "default_currency": "PKR",
        "csv_options": {"header": "true", "sep": ","},
        # raw_column -> canonical_column
        "column_map": {
            "transaction_id": "partner_txn_id",
            "phone_number": "msisdn_or_account",
            "event_type": "txn_type",
            "plan": "plan_code",
            "value": "amount",
            "ccy": "currency",
            "created_at": "txn_ts_local",
        },
        # raw txn_type value -> canonical txn_type
        "txn_type_map": {
            "SUB": "subscription_success",
            "RENEW_OK": "recursion_success",
            "RENEW_FAIL": "recursion_failure",
            "REFUND": "refund",
            "CANCEL": "cancel",
        },
        "ts_format": "yyyy-MM-dd HH:mm:ss",
    },

    # ---- Telco B: MNO, CSV, semicolon-delimited, different names --------
    "telco_b": {
        "file_format": "csv",
        "file_glob": "telco_b_*.csv",
        "timezone": "Asia/Karachi",
        "default_currency": "PKR",
        "csv_options": {"header": "true", "sep": ";"},
        "column_map": {
            "txn_ref": "partner_txn_id",
            "subscriber_id": "msisdn_or_account",
            "transaction_type": "txn_type",
            "product_code": "plan_code",
            "charged_amount": "amount",
            "currency_code": "currency",
            "event_time": "txn_ts_local",
        },
        "txn_type_map": {
            "initial": "subscription_success",
            "renewal_success": "recursion_success",
            "renewal_failure": "recursion_failure",
            "chargeback": "refund",
            "termination": "cancel",
        },
        "ts_format": "yyyy-MM-dd'T'HH:mm:ss",
    },

    # ---- Telco C: MNO, JSON feed ---------------------------------------
    "telco_c": {
        "file_format": "json",
        "file_glob": "telco_c_*.json",
        "timezone": "Asia/Dubai",             # UTC+4
        "default_currency": "AED",
        "csv_options": {},
        "column_map": {
            "id": "partner_txn_id",
            "account": "msisdn_or_account",
            "type": "txn_type",
            "tariff": "plan_code",
            "amt": "amount",
            "cur": "currency",
            "ts": "txn_ts_local",
        },
        "txn_type_map": {
            "new_sub": "subscription_success",
            "recur_ok": "recursion_success",
            "recur_ko": "recursion_failure",
            "refund": "refund",
            "cancel": "cancel",
        },
        "ts_format": "yyyy-MM-dd'T'HH:mm:ssXXX",  # ISO8601 w/ offset
    },

    # ---- Telco D: MNO, CSV, epoch-millis timestamps --------------------
    "telco_d": {
        "file_format": "csv",
        "file_glob": "telco_d_*.csv",
        "timezone": "Asia/Karachi",
        "default_currency": "PKR",
        "csv_options": {"header": "true", "sep": ","},
        "column_map": {
            "trx_id": "partner_txn_id",
            "msisdn": "msisdn_or_account",
            "kind": "txn_type",
            "bundle": "plan_code",
            "price": "amount",
            "currency": "currency",
            "epoch_ms": "txn_ts_local",       # special: epoch millis
        },
        "txn_type_map": {
            "S": "subscription_success",
            "R": "recursion_success",
            "F": "recursion_failure",
            "B": "refund",
            "C": "cancel",
        },
        "ts_format": "epoch_millis",          # handled specially in job
    },

    # ---- Wallet X: card/wallet partner, JSON, UTC already --------------
    "wallet_x": {
        "file_format": "json",
        "file_glob": "wallet_x_*.json",
        "timezone": "UTC",
        "default_currency": "USD",
        "csv_options": {},
        "column_map": {
            "payment_id": "partner_txn_id",
            "wallet_account": "msisdn_or_account",
            "category": "txn_type",
            "plan_ref": "plan_code",
            "gross_amount": "amount",
            "iso_currency": "currency",
            "processed_at": "txn_ts_local",
        },
        "txn_type_map": {
            "purchase": "subscription_success",
            "rebill_success": "recursion_success",
            "rebill_declined": "recursion_failure",
            "reversal": "refund",
            "cancellation": "cancel",
        },
        "ts_format": "yyyy-MM-dd'T'HH:mm:ss'Z'",
    },

    # ---- Wallet Y: card/wallet partner, CSV ----------------------------
    "wallet_y": {
        "file_format": "csv",
        "file_glob": "wallet_y_*.csv",
        "timezone": "Asia/Karachi",
        "default_currency": "PKR",
        "csv_options": {"header": "true", "sep": ","},
        "column_map": {
            "reference": "partner_txn_id",
            "customer_ref": "msisdn_or_account",
            "movement": "txn_type",
            "package": "plan_code",
            "total": "amount",
            "curr": "currency",
            "timestamp": "txn_ts_local",
        },
        "txn_type_map": {
            "signup_charge": "subscription_success",
            "auto_renew_ok": "recursion_success",
            "auto_renew_fail": "recursion_failure",
            "refund": "refund",
            "cancel": "cancel",
        },
        "ts_format": "yyyy-MM-dd HH:mm:ss",
    },
}


# The list of operators we currently ingest. Internal OLTP tables are
# suffixed with these (sub_initial_telco_a, sub_initial_telco_b, ...).
OPERATORS = list(OPERATOR_CONFIG.keys())


# ---------------------------------------------------------------------------
# Reconciliation matching configuration
# ---------------------------------------------------------------------------
# These knobs control the matching decision tree in the reconciliation
# engine. They live here (not buried in code) because Finance / Data will
# want to tune them, and keeping them as named values in one place means the
# "why this tolerance / window" answer always points at an explicit number.

RECON_CONFIG = {
    # Absolute currency tolerance when comparing partner vs platform amount.
    # 0.01 absorbs sub-cent rounding from FX / decimal storage differences.
    "amount_abs_tolerance": 0.01,

    # Relative tolerance (fraction). Either abs OR rel passing = "amounts equal".
    "amount_rel_tolerance": 0.005,           # 0.5%

    # How many days a partner txn can drift from the platform business_date
    # and still be considered the same calendar event. Operators run batch
    # windows that cross midnight in their local tz.
    "business_date_window_days": 1,

    # How late (in days) a partner record can arrive and still be folded
    # back into its ORIGINAL reconciliation period instead of being treated
    # as a brand-new break. Beyond this, it's an exception.
    "late_arrival_threshold_days": 2,

    # Fallback matching is risky (no shared id), so we require a tighter
    # business-date window for it to limit false positives.
    "fallback_business_date_window_days": 1,
}
