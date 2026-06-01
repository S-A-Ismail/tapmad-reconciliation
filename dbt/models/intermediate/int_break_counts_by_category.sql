-- Pivots fact_reconciliation_break into one row per (business_date, operator)
-- with a column per break category. Ephemeral: inlined into the mart.

with breaks as (
    select * from {{ ref('stg_reconciliation_break') }}
)

select
    business_date,
    operator_code,

    -- healthy
    count_if(recon_status = 'MATCHED')              as matched_count,
    count_if(recon_status = 'LATE_ARRIVAL')         as late_arrival_count,

    -- break categories the case study names explicitly
    count_if(recon_status = 'AMOUNT_MISMATCH')      as amount_mismatch_count,
    count_if(recon_status = 'MISSING_ON_PLATFORM')  as missing_on_platform_count,
    count_if(recon_status = 'MISSING_AT_PARTNER')   as missing_at_partner_count,
    count_if(recon_status = 'ORPHAN_CHURN')         as orphan_churn_count,

    -- any non-matched, non-late row is a "break" Finance must look at
    count_if(recon_status not in ('MATCHED', 'LATE_ARRIVAL')) as break_count,

    -- amount roll-ups straight off the fact (both sides carried per row)
    sum(partner_amount)   as partner_amount_total,
    sum(internal_amount)  as internal_amount_total,
    sum(amount_variance)  as variance

from breaks
group by business_date, operator_code
