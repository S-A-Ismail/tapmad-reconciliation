# Tapmad — Payments Reconciliation Pipeline

> Lead Data Engineer take-home, **Case Study A/B: Payments reconciliation across
> partner and internal systems.**

---

## TL;DR (30 seconds)

Tapmad bills subscribers through 6–7 operators. Every charge exists twice: once
at the **operator** (the source of truth for money) and once in the **internal
platform** (the source of truth for entitlement). The two drift. This pipeline
ingests both sides daily, normalizes 6–7 differently-shaped feeds into one
canonical schema, **matches partner ↔ platform transactions through a 3-tier
decision tree**, classifies every disagreement, and produces:

- `gold.fact_reconciliation_break` — one row per reconciled unit (the drill-down)
- `reconciliation_daily` — the summary **Finance opens each morning**
- `revenue_monthly_close` — the recognized-revenue figure for the **book close**

Built on **PySpark + Delta Lake + dbt + Airflow** (the stack I run today).
Tapmad runs Azure Fabric/Databricks — see [`docs/azure_migration_plan.md`](docs/azure_migration_plan.md)
for the from-scratch port. The logic moves over **unchanged**; only storage
paths and the orchestrator wrapper change.

---

## 1. How I interpreted the problem

The brief is really three problems wearing one coat:

1. **Integration** — 6–7 operator feeds, each a different shape/timezone/
   currency, plus a per-operator-suffixed internal OLTP. Get them into one
   comparable shape.
2. **Reconciliation** — decide what "the same transaction" *means* when the
   shared key is sometimes missing, amounts are off by rounding, and operators
   re-send last week's data. This is the hard part, and it's a **definitions**
   problem before it's a SQL problem.
3. **Trust** — Finance closes the books on this output and must be able to
   **restate any past day** with late data **without changing a closed month**.
   That makes idempotency and auditability first-class requirements, not nice-to-haves.

Everything below is organized around those three.

---

## 2. Architecture at a glance

A standard **medallion** (bronze → silver → gold) lakehouse. Full diagrams:
[`diagrams/architecture.md`](diagrams/architecture.md) · ERD:
[`diagrams/erd.md`](diagrams/erd.md).

```
operator feeds ─┐                                   ┌─ reconciliation_daily (Finance AM report)
                ├─ landing → bronze → silver ─┐      │
internal CDC  ──┘  (raw)    (deduped) (canonical)    ├─→ fact_reconciliation_break (drill-down)
                                                ├──► gold engine
                                                │     └─ revenue_monthly_close (book close)
```

| Layer | What it holds | Engine | Idempotency mechanism |
|------|----------------|--------|------------------------|
| **Landing** | raw files as dropped | — | n/a |
| **Bronze** | raw rows + ingest metadata, schema-on-read | PySpark | `MERGE` on natural key |
| **Silver** | canonical, UTC, normalized events | PySpark | `replaceWhere` per `business_date` |
| **Gold (fact)** | reconciliation results, per-row | PySpark | `replaceWhere` per `business_date` |
| **Gold (marts)** | daily summary + monthly close | dbt | incremental `replace_where` |

**Why this split of Spark vs dbt?** Bronze/silver/matching need imperative
control (timezone edge cases, multi-tier joins, window dedupe) — clearer and
faster in PySpark. The marts are SQL that **Finance can read, test, and trust**,
so dbt owns them with built-in tests and docs. This hybrid is exactly how I'd
run it on Databricks.

---

## 3. Integration — turning 6–7 shapes into one schema

The whole "different shape per operator" problem is solved with **one
config-driven job**, not seven bespoke ones. See
[`spark/config/operator_config.py`](spark/config/operator_config.py).

Each operator gets one dict entry describing its column names, txn-type codes,
file format, timezone, currency, and timestamp encoding:

```python
"telco_b": {
    "file_format": "csv", "csv_options": {"sep": ";"},
    "timezone": "Asia/Karachi", "default_currency": "PKR",
    "column_map": {"txn_ref": "partner_txn_id", "charged_amount": "amount", ...},
    "txn_type_map": {"renewal_success": "recursion_success", ...},
    "ts_format": "yyyy-MM-dd'T'HH:mm:ss",
}
```

[`silver_operator_feeds.py`](spark/silver/silver_operator_feeds.py) drives every
operator through the same 6 steps: rename → map txn types → parse timestamp →
**convert local time to UTC** → derive `business_date` → cast amount.
**Onboarding operator #8 = add one dict entry, write zero new code.**

Three timestamp encodings are handled explicitly (easy to get wrong):
naive local strings (converted from the operator's tz to UTC),
ISO-8601 with offset (already an instant), and epoch-millis (already an
instant). Getting `business_date` right is what makes the operator-local vs
platform-UTC reconciliation honest.

**The internal operator-suffixed tables** (`sub_initial_telco_a`,
`sub_initial_telco_b`, …) are handled by stamping `operator_code` at bronze
ingest and then **`UNION`-ing by logical table in silver**
([`silver_internal_events.py`](spark/silver/silver_internal_events.py)). I chose
*union-at-read with an operator column* over dynamic table discovery because
it's explicit, testable, and trivially extends to new operators — see
[§ Decision log](#7-decision-log).

---

## 4. Reconciliation — the matching decision tree

This is the core. Full implementation:
[`spark/gold/reconciliation_engine.py`](spark/gold/reconciliation_engine.py).
The brief's tip says it out loud: *the hard part is deciding what "matched"
means.* Here's the tree:

```
For partner event P and platform event I:

TIER 1 — STRONG KEY MATCH  (confidence: strong)
  P.partner_txn_id == I.partner_txn_id  (same operator)
  └─ amounts within tolerance?  → MATCHED
     else                       → AMOUNT_MISMATCH

TIER 2 — FALLBACK COMPOSITE  (confidence: fallback)
  only when I.partner_txn_id IS NULL (key never existed to join on)
  match on operator + txn_type + business_date(±1d) + amount(in tolerance)
  guard fan-out: keep the single closest-amount candidate (row_number=1)
  → MATCHED, tagged match_confidence='fallback' so Finance sees the heuristic

TIER 3 — NO MATCH → classify the survivor
  partner row,  no platform counterpart → MISSING_ON_PLATFORM
  platform row, no partner counterpart  → MISSING_AT_PARTNER
        (recursion_failure excluded — an expected non-charge, not a break)

CROSS-CUTTING
  ORPHAN_CHURN  — user churned on platform but operator still billed → re-tag
  LATE_ARRIVAL  — file_arrival_date > business_date + 2d; folded into the
                  ORIGINAL period and flagged (closed months restate cleanly)
```

**Amount tolerance** (`RECON_CONFIG` in operator_config.py): a match passes if
*either* the absolute diff ≤ 0.01 *or* the relative diff ≤ 0.5%. That absorbs
sub-cent decimal/FX rounding without masking real discrepancies. Every number
is a named config value in one place, so the thresholds are easy to find and tune.

**False-positive control on the fallback tier:** no shared id means risk, so I
require amount agreement **and** a tight ±1-day window **and** a 1:1 pairing
(the closest-amount candidate wins; no fan-out). The match is then *labeled*
`fallback` so it's never silently trusted.

The output `fact_reconciliation_break` carries **both** the partner and internal
amount on every row, so the marts compute totals/variance without re-joining,
and every row keeps `partner_txn_id` / `platform_event_id` as an **audit trail
back to source**.

---

## 5. Trust — idempotency, late arrivals, restatement

These three requirements are the difference between a demo and something
Finance can close books on.

**Idempotency (operators re-send 3 days of corrections).**
- Bronze `MERGE`s on the natural key → a re-sent row updates in place, never
  appends a duplicate. ([`delta_utils.merge_upsert`](spark/utils/delta_utils.py))
- Silver/Gold use Delta **`replaceWhere "business_date = X"`** → recomputing a
  day atomically swaps *only that day's* partition. Re-running is deterministic.
- A dbt data test [`assert_no_double_counting.sql`](dbt/tests/assert_no_double_counting.sql)
  fails the build if any `partner_txn_id` is ever counted twice.

**Late arrivals (txn lands 2+ days after close).** `business_date` is derived
from the **transaction's UTC time**, *not* its file-arrival date. So a row that
lands late still carries its original `business_date`; the backfill re-runs that
day, `replaceWhere` swaps the partition, and the number **restates correctly**.
The row is additionally tagged `LATE_ARRIVAL` / `is_late_arrival = true` so
Finance knows a closed period received late data.

**Re-statable history without disturbing closed months.** Because each
`business_date` is an independent partition and every write is a scoped replace,
re-running 2024-01-10 touches *only* 2024-01-10. The Airflow backfill of any
past date is a clean restatement — closed months are never mutated as a side
effect. (For a hard freeze, add a `closed_periods` guard table; noted in
[§ With more time](#9-what-id-do-with-more-time).)

---

## 6. The final deliverables (mapped to the brief)

The brief asks for a `reconciliation_daily` mart with specific columns and an
FK to a break-detail table. Delivered:

**`reconciliation_daily`** ([model](dbt/models/marts/reconciliation_daily.sql)) — one row per `(business_date, operator_code)`:
`partner_txn_count`, `internal_txn_count`, `matched_count`, `break_count`,
`partner_amount_total`, `internal_amount_total`, `variance`, plus counts by
category (`amount_mismatch_count`, `missing_on_platform_count`,
`missing_at_partner_count`, `orphan_churn_count`) and a `match_rate` for
alerting. Drill down via `(business_date, operator_code)` into…

**`fact_reconciliation_break`** ([engine](spark/gold/reconciliation_engine.py)) — the individual mismatched rows, with `recon_status`,
`match_confidence`, both amounts, variance, and source ids for audit.

**`revenue_monthly_close`** ([model](dbt/models/marts/revenue_monthly_close.sql)) — recognized revenue per month/operator with an explicit,
documented recognition policy and audit columns showing how much rests on
imperfect rows.

---

## 7. Decision log

| Decision | Chose | Over | Why |
|----------|-------|------|-----|
| Operator-suffixed table join | `UNION` + `operator_code` column at bronze | dynamic table discovery | explicit, testable, schema-on-read; new operator = config only |
| Normalization placement | silver, config-driven | per-operator jobs | one code path; 7 shapes is data, not branches |
| `business_date` source | transaction UTC time | file_arrival_date | makes late arrivals restate into the right period |
| Matching when key missing | composite fallback, **labeled & guarded** | drop, or trust blindly | recovers real matches without hiding the risk |
| Amount equality | abs 0.01 **or** rel 0.5% | exact equality | absorbs rounding/FX without masking breaks |
| recursion_failure | excluded from "missing at partner" | treat as break | it's an expected non-charge; otherwise false breaks |
| Idempotency | Delta `MERGE` + `replaceWhere` | append + dedupe later | deterministic re-runs, no double count, atomic |
| Marts engine | dbt | more PySpark | SQL Finance can read + tested + documented |
| Spark for matching | PySpark | dbt SQL | window dedupe + multi-tier joins clearer/faster |

---

## 8. How to run

### Option A — fully local, no cloud, no warehouse (fastest demo)
```bash
python -m venv .venv && source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt

# generate data + run bronze→silver→gold + print a reconciliation summary
python run_local.py --business-date 2024-01-15 --n 300
```
You'll see a per-operator breakdown of MATCHED / AMOUNT_MISMATCH /
MISSING_ON_PLATFORM / MISSING_AT_PARTNER / ORPHAN_CHURN / LATE_ARRIVAL plus
partner/internal totals and variance — i.e. `reconciliation_daily` previewed in
Spark.

### Option B — build the dbt marts (needs a Databricks connection)
```bash
cd dbt
cp profiles.yml.example ~/.dbt/profiles.yml   # fill in host/http_path/token
dbt deps
dbt build --vars '{business_date: 2024-01-15}'   # runs models + tests
```

### Option C — orchestrated via Airflow
Point Airflow at `airflow/dags/`; the `tapmad_reconciliation_daily` DAG runs the
whole chain and `dbt build`. Backfill any date to restate it.

### Option D — everything in Docker (no local Python/Java/Spark needed)
The whole stack is containerized: the PySpark jobs and dbt run in one image
(Spark in `local[*]` mode), Airflow runs in its own image, and all services
share a `lakehouse` Delta volume. dbt's `local` target uses **dbt-spark's
session method** against the in-container Spark, so the marts build fully
offline (no warehouse required); `spark/register_tables.py` exposes the silver/
gold Delta paths to dbt via a local Hive metastore.

```bash
docker compose build

# run the pipeline (generate -> bronze -> silver -> gold -> preview)
docker compose run --rm pipeline

# build the dbt marts + tests (registers tables, then dbt build on local Spark)
docker compose run --rm dbt

# or the full Airflow experience: UI at http://localhost:8080 (airflow / airflow)
docker compose --profile airflow up -d
```

`make build` / `make pipeline` / `make dbt` / `make airflow-up` wrap these.
To point dbt at a real warehouse instead: `DBT_TARGET=databricks` plus
`DATABRICKS_HOST` / `DATABRICKS_HTTP_PATH` / `DATABRICKS_TOKEN`.

> **Note on running code:** Option A runs the whole thing end-to-end on a
> laptop. The synthetic generator plants every reconciliation scenario, so each
> `recon_status` shows up in the output and you can see the classification work.

---

## 9. What I'd do with more time

Honest list of what I deliberately deferred under the 72-hour window:

- **`closed_periods` freeze table** — a hard guard so a backfill into a
  *signed-off* month requires an explicit adjustment record rather than silently
  restating. Today closed months are protected by partition isolation but not
  formally locked.
- **Currency → reporting currency** — I normalize to operator-local currency and
  reconcile within an operator. A real close needs an FX-rate dimension to roll
  multi-currency operators into one reporting currency (PKR/USD).
- **Auto Loader / DLT ingestion** on Azure instead of the local `glob` reader,
  with schema-evolution and exactly-once file tracking.
- **Fuzzy MSISDN resolution** for the fallback tier (number portability,
  account-id changes) backed by a stable user-mapping dimension.
- **SCD2 on subscription state** to answer "what was the entitlement on date X"
  for historical restatement, not just current status.
- **Data-quality expectations** (Great Expectations / DLT expectations) on the
  bronze→silver boundary, surfacing the `unknown` txn_type bucket as alerts.
- **Reconciliation SLAs & alerting** — page when `match_rate` drops below a
  per-operator threshold or `break_count` spikes.
- **Property-based tests** on the matching engine (generate adversarial
  amount/date/key combinations and assert classification invariants).

---

## 10. Repository layout

```
tapmad-reconciliation/
├── README.md                        ← you are here
├── run_local.py                     ← one-shot local end-to-end runner
├── requirements.txt
├── docker-compose.yml               ← full stack: pipeline + dbt + Airflow
├── Makefile                         ← make build / pipeline / dbt / airflow-up
├── docker/                          ← Dockerfiles, spark-defaults, jar warm-up
├── data/synthetic/generate_data.py  ← plants every reconciliation scenario
├── spark/
│   ├── config/operator_config.py    ← the 6-7 operator shapes + recon knobs
│   ├── config/paths.py              ← medallion paths (only file Azure changes)
│   ├── utils/                       ← spark session + Delta idempotency helpers
│   ├── ingestion/                   ← bronze: operator feeds + internal CDC
│   ├── silver/                      ← canonical normalization + union
│   └── gold/reconciliation_engine.py← THE matching decision tree
├── dbt/
│   ├── models/staging/              ← typed views over silver + gold fact
│   ├── models/intermediate/         ← pivots + independent source counts
│   ├── models/marts/                ← reconciliation_daily, revenue_monthly_close
│   └── tests/                       ← no-double-counting, matched-balance
├── airflow/dags/                    ← daily DAG (= backfill/restatement)
├── diagrams/                        ← architecture + ERD (Mermaid)
└── docs/azure_migration_plan.md     ← from-scratch port to Fabric/Databricks
```

---

## 11. A note on the stack choice

The brief prefers PySpark + Delta on Azure Fabric and says alternatives are fine
with an explanation of how they translate. I used **open-source Spark + Delta +
dbt + Airflow** because that's what I run day-to-day and it lets the whole thing
run locally. The Spark and Delta fundamentals are all
here — config-driven normalization, joins across operator-suffixed tables,
window-function dedupe, partitioning by `business_date`, and idempotent
`MERGE` / `replaceWhere` writes. The port to Azure is mechanical and fully
specified in [`docs/azure_migration_plan.md`](docs/azure_migration_plan.md):
business logic unchanged, only storage paths and the orchestrator wrapper move.
```
