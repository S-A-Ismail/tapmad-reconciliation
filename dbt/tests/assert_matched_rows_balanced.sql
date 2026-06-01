-- DATA TEST: any row classified MATCHED must actually have partner and
-- internal amounts within tolerance. If this returns rows, the classification
-- logic and the amount comparison have drifted apart — a correctness bug.

select
    business_date,
    operator_code,
    partner_txn_id,
    partner_amount,
    internal_amount,
    amount_variance
from {{ ref('stg_reconciliation_break') }}
where recon_status = 'MATCHED'
  and abs(amount_variance) > {{ var('amount_abs_tolerance') }}
