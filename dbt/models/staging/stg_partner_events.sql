-- Canonical partner (operator) events. Used by reconciliation_daily to count
-- the raw partner-side population independently of the match outcome, so we
-- can report partner_txn_count vs internal_txn_count honestly.

select
    business_date,
    operator_code,
    partner_txn_id,
    msisdn_or_account,
    txn_type,
    cast(amount as decimal(12,2)) as amount,
    currency,
    txn_ts_utc,
    file_arrival_date
from {{ source('silver', 'partner_events') }}
where txn_type in ('subscription_success', 'recursion_success', 'refund')
