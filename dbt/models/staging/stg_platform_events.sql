-- Canonical platform (internal) events. Money-moving event types only, so the
-- internal_txn_count in the mart is comparable to the partner side. Recursion
-- failures are excluded here (expected non-charges).

select
    business_date,
    operator_code,
    platform_event_id,
    event_type,
    sub_id,
    user_id,
    partner_txn_id,
    cast(amount as decimal(12,2)) as amount,
    event_ts_utc
from {{ source('silver', 'platform_events') }}
where event_type in ('subscription_success', 'recursion_success')
