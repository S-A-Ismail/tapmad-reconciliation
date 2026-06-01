# scratch notes

Running notes to self while building this out. Not polished - the real
write-up is in the README.

## open questions I'd ask the team
- Is `partner_txn_id` ever reused across operators, or globally unique? I'm
  assuming unique *within* an operator and joining on (operator_code, txn_id).
- When `partner_txn_id` is null on the internal side, is there any other stable
  id I'm missing? Right now I fall back to operator+user+type+amount+date.
- What's the actual close cut-off time? I'm bucketing on UTC business_date but
  Finance might define the day in PKT. Easy to switch, just need the rule.
- Refunds: does the operator send a negative amount or a separate refund row
  with txn_type=refund? I assumed a separate row.

## decisions I went back and forth on
- thought about doing the matching in dbt too, but the fallback tier needs a
  window function + a 1:1 guard and it was just cleaner in pyspark. left the
  marts in dbt.
- almost partitioned bronze by ingest date only, switched to
  (operator_code, file_arrival_date) so re-sends overwrite the right slice.
- tolerance: started at exact match, immediately blew up on rounding in the
  fake data. 0.01 abs / 0.5% rel felt right, made it config.

## TODO if I had more time
- closed_periods freeze table (see README sec 9)
- FX dimension for multi-currency roll-up
- a few more dbt tests on the silver layer (not_null on business_date etc)
- wire up the orphan_churn overlay more carefully, the vice-versa case
  (churned at partner, still entitled on platform) is only half-handled

## gotchas hit
- telco_d sends epoch millis as a string, had to cast before /1000
- telco_c timestamps carry a +04:00 offset so they're already instants - don't
  double-convert them through the tz like the naive ones
- json feeds need newline-delimited for spark to read them as multiple rows
