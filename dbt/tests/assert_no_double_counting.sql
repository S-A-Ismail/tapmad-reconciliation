-- DATA TEST: a partner transaction must never be counted twice in the fact.
-- This is the guardrail behind the "operators re-send 3 days of corrections"
-- requirement. If bronze MERGE + silver replaceWhere are working, every
-- (operator_code, partner_txn_id) appears at most once with a partner_amount.
-- The test FAILS (returns rows) if any partner_txn_id is duplicated.

select
    operator_code,
    partner_txn_id,
    count(*) as occurrences
from {{ ref('stg_reconciliation_break') }}
where partner_txn_id is not null
  and partner_amount > 0
group by operator_code, partner_txn_id
having count(*) > 1
