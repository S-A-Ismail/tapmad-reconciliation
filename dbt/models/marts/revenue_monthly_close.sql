{{
  config(
    materialized='table',
    file_format='delta'
  )
}}

-- =====================================================================
-- revenue_monthly_close  —  the figure Finance books each month
-- =====================================================================
-- Recognized revenue per (month, operator), built ONLY from reconciled,
-- money-moving partner transactions, with the audit trail preserved:
-- every dollar here traces to rows in gold.fact_reconciliation_break via
-- (business_date, operator_code, partner_txn_id).
--
-- Recognition policy (documented so the booked number is auditable):
--   * MATCHED and LATE_ARRIVAL rows ARE recognized (money confirmed on both
--     sides; late ones simply arrived after the fact but belong to the month).
--   * AMOUNT_MISMATCH is recognized at the PARTNER amount (the money that
--     actually moved is the operator's number) and flagged for review.
--   * MISSING_ON_PLATFORM is recognized (operator billed real money) but
--     flagged — entitlement is broken, finance still received cash.
--   * MISSING_AT_PARTNER is NOT recognized (no money moved).
--   * Refunds reduce revenue (negative).
-- =====================================================================

with breaks as (
    select * from {{ ref('stg_reconciliation_break') }}
),

recognized as (
    select
        date_trunc('month', business_date)          as close_month,
        operator_code,
        partner_txn_id,
        recon_status,
        txn_type,
        -- amount actually moved at the operator is the source of truth for cash
        case
            when txn_type = 'refund' then -1 * partner_amount
            else partner_amount
        end                                         as recognized_amount,
        partner_amount,
        internal_amount,
        amount_variance,
        business_date
    from breaks
    where recon_status in (
        'MATCHED', 'LATE_ARRIVAL', 'AMOUNT_MISMATCH', 'MISSING_ON_PLATFORM'
    )
)

select
    close_month,
    operator_code,

    count(*)                                        as recognized_txn_count,
    sum(recognized_amount)                          as recognized_revenue,

    -- transparency: how much of the booked number rests on imperfect rows
    sum(case when recon_status = 'AMOUNT_MISMATCH'
             then partner_amount else 0 end)        as revenue_from_mismatched,
    sum(case when recon_status = 'MISSING_ON_PLATFORM'
             then partner_amount else 0 end)        as revenue_from_unentitled,
    sum(abs(amount_variance))                       as total_abs_variance,

    current_timestamp()                             as _built_at

from recognized
group by close_month, operator_code
