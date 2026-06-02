# Sample landing data

A small synthetic dataset for `business_date = 2024-01-15` (~150 transactions),
produced by [`../synthetic/generate_data.py`](../synthetic/generate_data.py). It
exists so the pipeline has something to read out of the box and so you can see
the raw operator/internal shapes without running the generator.

```
landing/
  operator_feeds/<operator>/     partner feeds in each operator's RAW shape
  internal/<table>_<operator>/   internal OLTP CDC (JSON)
```

Note the per-operator differences that silver normalizes away:
- `telco_a` CSV comma, `telco_b` CSV semicolon, `telco_d` CSV with epoch-millis
- `telco_c` JSON with a `+04:00` offset, `wallet_x` JSON in UTC (`Z`)
- the `telco_*_20240118.csv` files are **late arrivals**: rows whose
  `business_date` is 2024-01-15 but that landed on the 18th.

Every reconciliation scenario is planted here (matched, amount_mismatch,
missing_on_platform, missing_at_partner, orphan_churn, late_arrival,
null partner_txn_id, duplicate re-send).

Regenerate / make more:
```bash
LAKEHOUSE_ROOT=data/sample python data/synthetic/generate_data.py \
  --business-date 2024-01-15 --n 150
```
