-- Thin, typed view over the Spark-produced fact table. Staging's only job is
-- to rename/clean and present a stable contract to the marts. No business
-- logic here.

with source as (
    select * from {{ source('gold', 'fact_reconciliation_break') }}
)

select
    business_date,
    operator_code,
    recon_status,
    match_confidence,
    match_key,
    partner_txn_id,
    platform_event_id,
    user_id,
    msisdn_or_account,
    txn_type,
    cast(partner_amount  as decimal(12,2)) as partner_amount,
    cast(internal_amount as decimal(12,2)) as internal_amount,
    cast(amount_variance as decimal(12,2)) as amount_variance,
    currency,
    partner_txn_ts_utc,
    platform_event_ts_utc,
    file_arrival_date,
    is_late_arrival,
    recon_run_ts
from source
