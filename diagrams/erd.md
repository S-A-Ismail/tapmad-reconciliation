# Data Model (ERD)

Two parallel canonical streams (partner vs platform) feed the reconciliation
fact, which rolls up into the daily mart and the monthly close.

```mermaid
erDiagram
    SILVER_PARTNER_EVENTS {
        string operator_code
        string partner_txn_id PK
        string msisdn_or_account
        string txn_type
        string plan_code
        decimal amount
        string currency
        timestamp txn_ts_utc
        timestamp txn_ts_local
        date business_date
        date file_arrival_date
    }

    SILVER_PLATFORM_EVENTS {
        string operator_code
        string platform_event_id PK
        string event_type
        string sub_id
        string user_id
        string partner_txn_id FK
        string plan_id
        decimal amount
        timestamp event_ts_utc
        date business_date
    }

    FACT_RECONCILIATION_BREAK {
        date business_date PK
        string operator_code PK
        string recon_status
        string match_confidence
        string match_key
        string partner_txn_id FK
        string platform_event_id FK
        string user_id
        string txn_type
        decimal partner_amount
        decimal internal_amount
        decimal amount_variance
        boolean is_late_arrival
        timestamp recon_run_ts
    }

    RECONCILIATION_DAILY {
        date business_date PK
        string operator_code PK
        long partner_txn_count
        long internal_txn_count
        long matched_count
        long break_count
        decimal partner_amount_total
        decimal internal_amount_total
        decimal variance
        long amount_mismatch_count
        long missing_on_platform_count
        long missing_at_partner_count
        long orphan_churn_count
        double match_rate
    }

    REVENUE_MONTHLY_CLOSE {
        date close_month PK
        string operator_code PK
        long recognized_txn_count
        decimal recognized_revenue
        decimal revenue_from_mismatched
        decimal revenue_from_unentitled
        decimal total_abs_variance
    }

    SILVER_PARTNER_EVENTS   ||--o{ FACT_RECONCILIATION_BREAK : "matched/unmatched"
    SILVER_PLATFORM_EVENTS  ||--o{ FACT_RECONCILIATION_BREAK : "matched/unmatched"
    FACT_RECONCILIATION_BREAK ||--|| RECONCILIATION_DAILY : "aggregates to"
    FACT_RECONCILIATION_BREAK ||--|| REVENUE_MONTHLY_CLOSE : "recognizes from"
```

## Grain notes
- `silver_partner_events`: one row per operator transaction (money moved).
- `silver_platform_events`: one row per internal money-moving event
  (subscription_success / recursion_success), unioned across all
  operator-suffixed OLTP tables. recursion_failure carried separately for
  explanation, churn carried for orphan detection.
- `fact_reconciliation_break`: one row per reconciled unit — a matched pair,
  an unmatched partner row, or an unmatched platform row. This is the
  drill-down detail; `partner_txn_id` / `platform_event_id` trace to source.
- `reconciliation_daily`: one row per (business_date, operator_code).
- `revenue_monthly_close`: one row per (close_month, operator_code).
```
