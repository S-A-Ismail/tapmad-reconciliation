-- Independent counts of the raw partner and platform populations per
-- (business_date, operator). These are computed straight from the silver
-- streams, NOT from the matched fact, so partner_txn_count / internal_txn_count
-- are an honest "how many money-moving rows existed on each side" — which is
-- exactly what Finance needs to sanity-check the reconciliation totals.

with partner as (
    select
        business_date,
        operator_code,
        count(*)            as partner_txn_count,
        sum(amount)         as partner_amount_raw
    from {{ ref('stg_partner_events') }}
    group by business_date, operator_code
),

platform as (
    select
        business_date,
        operator_code,
        count(*)            as internal_txn_count,
        sum(amount)         as internal_amount_raw
    from {{ ref('stg_platform_events') }}
    group by business_date, operator_code
)

select
    coalesce(p.business_date, i.business_date)   as business_date,
    coalesce(p.operator_code, i.operator_code)   as operator_code,
    coalesce(p.partner_txn_count, 0)             as partner_txn_count,
    coalesce(i.internal_txn_count, 0)            as internal_txn_count,
    coalesce(p.partner_amount_raw, 0)            as partner_amount_raw,
    coalesce(i.internal_amount_raw, 0)           as internal_amount_raw
from partner p
full outer join platform i
    on  p.business_date  = i.business_date
    and p.operator_code  = i.operator_code
