"""
Synthetic data generator.
========================================================================

Produces, for a given business date:
  * one PARTNER feed per operator, in that operator's RAW shape (raw column
    names, raw txn_type codes, operator-specific timestamp format / timezone),
    written to landing/operator_feeds/<operator>/
  * the INTERNAL OLTP CDC files (sub_initial_<op>, sub_recursion_success_<op>,
    sub_recursion_failure_<op>, user_churn_events) as JSON, written to
    landing/internal/

The generator deliberately plants every reconciliation scenario from the brief
so the pipeline visibly detects each one:

    matched               both sides agree
    amount_mismatch       same txn, different amount
    missing_on_platform   partner billed, no internal row
    missing_at_partner    internal entitlement, no partner row
    orphan_churn          churned user still billed by operator
    late_arrival          partner row with file_arrival_date >> business_date
    null_partner_txn_id   internal row missing the key -> forces fallback match
    duplicate_resend      operator re-sends a row (tests idempotency)

Keep it small and fast — the brief says don't over-invest in fake data.

Usage:
    python data/synthetic/generate_data.py --business-date 2024-01-15 --n 200
"""

import argparse
import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from spark.config import paths  # noqa: E402
from spark.config.operator_config import OPERATOR_CONFIG  # noqa: E402

random.seed(42)

# Per-operator reverse maps: canonical -> raw, so we can emit raw shapes.
def _raw_txn_type(op_cfg, canonical):
    for raw, canon in op_cfg["txn_type_map"].items():
        if canon == canonical:
            return raw
    return list(op_cfg["txn_type_map"].keys())[0]


def _fmt_ts(op_cfg, dt):
    """Render a datetime in the operator's raw timestamp encoding."""
    fmt = op_cfg["ts_format"]
    if fmt == "epoch_millis":
        return str(int(dt.timestamp() * 1000))
    if fmt.endswith("'Z'") or fmt == "yyyy-MM-dd'T'HH:mm:ss'Z'":
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if "XXX" in fmt:
        # ISO8601 with a +HH:MM offset (telco_c / Dubai = +04:00)
        return dt.strftime("%Y-%m-%dT%H:%M:%S") + "+04:00"
    if "'T'" in fmt:
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


SCENARIOS = [
    ("matched", 0.62),
    ("amount_mismatch", 0.08),
    ("missing_on_platform", 0.08),
    ("missing_at_partner", 0.07),
    ("orphan_churn", 0.05),
    ("late_arrival", 0.04),
    ("null_partner_txn_id", 0.04),
    ("duplicate_resend", 0.02),
]


def _pick_scenario():
    r = random.random()
    acc = 0.0
    for name, w in SCENARIOS:
        acc += w
        if r <= acc:
            return name
    return "matched"


def generate(business_date: str, n: int):
    bd = datetime.strptime(business_date, "%Y-%m-%d")
    paths.ensure_dirs()

    # accumulate rows per output file
    partner_rows = {op: [] for op in OPERATOR_CONFIG}
    sub_initial = {op: [] for op in OPERATOR_CONFIG}
    recursion_success = {op: [] for op in OPERATOR_CONFIG}
    recursion_failure = {op: [] for op in OPERATOR_CONFIG}
    churn_rows = []

    operators = list(OPERATOR_CONFIG.keys())

    for _ in range(n):
        op = random.choice(operators)
        cfg = OPERATOR_CONFIG[op]
        scenario = _pick_scenario()

        txn_canon = random.choice(["subscription_success", "recursion_success"])
        ptxn = f"{op}-{uuid.uuid4().hex[:12]}"
        user_id = f"u-{uuid.uuid4().hex[:10]}"
        account = f"acct-{random.randint(10**9, 10**10 - 1)}"
        plan = random.choice(["P_BASIC", "P_STD", "P_PREMIUM"])
        amount = round(random.choice([99.0, 149.0, 299.0, 499.0]), 2)
        # event time during the business day, in operator-local wall clock
        evt = bd + timedelta(
            hours=random.randint(0, 23), minutes=random.randint(0, 59)
        )
        arrival = business_date  # default: same day

        def emit_partner(amt, arr):
            row = {
                cfg["column_map_inv"]["partner_txn_id"]: ptxn,
                cfg["column_map_inv"]["msisdn_or_account"]: account,
                cfg["column_map_inv"]["txn_type"]: _raw_txn_type(cfg, txn_canon),
                cfg["column_map_inv"]["plan_code"]: plan,
                cfg["column_map_inv"]["amount"]: amt,
                cfg["column_map_inv"]["currency"]: cfg["default_currency"],
                cfg["column_map_inv"]["txn_ts_local"]: _fmt_ts(cfg, evt),
            }
            partner_rows[op].append((row, arr))

        def emit_internal(ptxn_value):
            if txn_canon == "subscription_success":
                sub_initial[op].append({
                    "sub_id": f"sub-{uuid.uuid4().hex[:10]}",
                    "user_id": user_id,
                    "operator_code": op,
                    "plan_id": plan,
                    "partner_txn_id": ptxn_value,
                    "status": "active",
                    "created_ts": evt.strftime("%Y-%m-%dT%H:%M:%S"),
                    "amount": amount,
                })
            else:
                recursion_success[op].append({
                    "recursion_id": f"rec-{uuid.uuid4().hex[:10]}",
                    "sub_id": f"sub-{uuid.uuid4().hex[:10]}",
                    "user_id": user_id,
                    "operator_code": op,
                    "partner_txn_id": ptxn_value,
                    "amount": amount,
                    "recurrence_ts": evt.strftime("%Y-%m-%dT%H:%M:%S"),
                    "billing_cycle": random.randint(1, 12),
                })

        # ---- scenario wiring -------------------------------------------
        if scenario == "matched":
            emit_partner(amount, arrival)
            emit_internal(ptxn)
        elif scenario == "amount_mismatch":
            emit_partner(amount, arrival)
            emit_internal(ptxn)  # internal keeps `amount`
            # bump partner amount so the two disagree
            partner_rows[op][-1][0][cfg["column_map_inv"]["amount"]] = round(amount + 50.0, 2)
        elif scenario == "missing_on_platform":
            emit_partner(amount, arrival)  # partner only, no internal row
        elif scenario == "missing_at_partner":
            emit_internal(ptxn)            # internal only, no partner row
        elif scenario == "orphan_churn":
            emit_partner(amount, arrival)
            emit_internal(ptxn)
            churn_rows.append({
                "user_id": user_id,
                "operator_code": op,
                "churn_ts": (bd - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S"),
                "churn_reason": random.choice(["voluntary", "support_ticket"]),
                "last_known_sub_id": f"sub-{uuid.uuid4().hex[:10]}",
            })
        elif scenario == "late_arrival":
            # partner row belongs to today's business_date but lands 3 days later
            emit_partner(amount, (bd + timedelta(days=3)).strftime("%Y-%m-%d"))
            emit_internal(ptxn)
        elif scenario == "null_partner_txn_id":
            # partner has the key; internal row's key is NULL -> fallback match
            emit_partner(amount, arrival)
            emit_internal(None)
        elif scenario == "duplicate_resend":
            emit_partner(amount, arrival)
            emit_partner(amount, arrival)  # same ptxn twice -> idempotency test
            emit_internal(ptxn)

        # sprinkle some recursion failures (expected non-charges)
        if random.random() < 0.05:
            recursion_failure[op].append({
                "failure_id": f"fail-{uuid.uuid4().hex[:10]}",
                "sub_id": f"sub-{uuid.uuid4().hex[:10]}",
                "user_id": f"u-{uuid.uuid4().hex[:10]}",
                "operator_code": op,
                "failure_reason": random.choice(
                    ["insufficient_balance", "partner_timeout", "user_cancelled"]
                ),
                "attempt_ts": evt.strftime("%Y-%m-%dT%H:%M:%S"),
                "retry_count": random.randint(0, 3),
            })

    _write_partner_files(partner_rows, business_date)
    _write_internal_files(
        sub_initial, recursion_success, recursion_failure, churn_rows, business_date
    )
    print(f"[gen] wrote synthetic data for {business_date} (n={n})")


def _write_partner_files(partner_rows, business_date):
    for op, rows in partner_rows.items():
        if not rows:
            continue
        cfg = OPERATOR_CONFIG[op]
        # group rows by their (possibly future) arrival date
        by_arrival = {}
        for row, arrival in rows:
            by_arrival.setdefault(arrival, []).append(row)

        out_dir = os.path.join(paths.LANDING_OPERATOR, op)
        os.makedirs(out_dir, exist_ok=True)

        for arrival, rws in by_arrival.items():
            stamp = arrival.replace("-", "")
            if cfg["file_format"] == "csv":
                sep = cfg.get("csv_options", {}).get("sep", ",")
                fname = os.path.join(out_dir, f"{op}_{stamp}.csv")
                with open(fname, "w", newline="", encoding="utf-8") as fh:
                    w = csv.DictWriter(fh, fieldnames=list(rws[0].keys()), delimiter=sep)
                    w.writeheader()
                    w.writerows(rws)
            else:  # json (newline-delimited for Spark)
                fname = os.path.join(out_dir, f"{op}_{stamp}.json")
                with open(fname, "w", encoding="utf-8") as fh:
                    for r in rws:
                        fh.write(json.dumps(r) + "\n")


def _write_internal_files(sub_initial, rec_succ, rec_fail, churn_rows, business_date):
    stamp = business_date.replace("-", "")

    def dump(per_op, logical):
        for op, rows in per_op.items():
            if not rows:
                continue
            d = os.path.join(paths.LANDING_INTERNAL, f"{logical}_{op}")
            os.makedirs(d, exist_ok=True)
            with open(os.path.join(d, f"{logical}_{op}_{stamp}.json"), "w",
                      encoding="utf-8") as fh:
                for r in rows:
                    fh.write(json.dumps(r) + "\n")

    dump(sub_initial, "sub_initial")
    dump(rec_succ, "sub_recursion_success")
    dump(rec_fail, "sub_recursion_failure")

    if churn_rows:
        d = os.path.join(paths.LANDING_INTERNAL, "user_churn_events")
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, f"user_churn_events_{stamp}.json"), "w",
                  encoding="utf-8") as fh:
            for r in churn_rows:
                fh.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    # add inverse column map (canonical -> raw) onto each cfg for convenience
    for _op, _cfg in OPERATOR_CONFIG.items():
        _cfg["column_map_inv"] = {v: k for k, v in _cfg["column_map"].items()}

    ap = argparse.ArgumentParser()
    ap.add_argument("--business-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--n", type=int, default=200, help="approx rows to generate")
    args = ap.parse_args()
    generate(args.business_date, args.n)
