-- Pivots fact_reconciliation_break into one row per (business_date, operator)
-- with a column per break category. Ephemeral: inlined into the mart.

with breaks as (
    select * from {{ ref('stg_reconciliation_break') }}
)

select
    business_date,
    operator_code,

    -- healthy
    count(case when recon_status = 'MATCHED' then 1 end)              as matched_count,
    count(case when recon_status = 'LATE_ARRIVAL' then 1 end)         as late_arrival_count,

    -- break categories the case study names explicitly
    count(case when recon_status = 'AMOUNT_MISMATCH' then 1 end)      as amount_mismatch_count,
    count(case when recon_status = 'MISSING_ON_PLATFORM' then 1 end)  as missing_on_platform_count,
    count(case when recon_status = 'MISSING_AT_PARTNER' then 1 end)   as missing_at_partner_count,
    count(case when recon_status = 'ORPHAN_CHURN' then 1 end)         as orphan_churn_count,

    -- any non-matched, non-late row is a "break" Finance must look at
    count(case when recon_status not in ('MATCHED', 'LATE_ARRIVAL') then 1 end) as break_count,

    -- amount roll-ups straight off the fact (both sides carried per row)
    sum(partner_amount)   as partner_amount_total,
    sum(internal_amount)  as internal_amount_total,
    sum(amount_variance)  as variance

from breaks
group by business_date, operator_code
