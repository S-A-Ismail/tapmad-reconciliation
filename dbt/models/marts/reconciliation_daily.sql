{%- set inc_strategy = var('dbt_incremental_strategy', 'replace_where') -%}
{{
  config(
    materialized='incremental',
    incremental_strategy=inc_strategy,
    incremental_predicates=(
      ["business_date in (select distinct business_date from " ~ this ~ ")"]
      if inc_strategy == 'replace_where' else none
    ),
    file_format='delta',
    partition_by=['business_date'],
    unique_key=['business_date', 'operator_code']
  )
}}

-- Idempotency strategy is portable:
--   * Databricks (default): replace_where on business_date.
--   * open-source Spark (local container): insert_overwrite does dynamic
--     partition overwrite on business_date - same per-day atomic replace.
-- Pass --vars '{dbt_incremental_strategy: insert_overwrite}' for the local run.

-- =====================================================================
-- reconciliation_daily  —  THE TABLE FINANCE OPENS EACH MORNING
-- =====================================================================
-- One row per (business_date, operator_code) summarizing the day's
-- reconciliation: how many transactions each side had, how many matched,
-- how many broke and in which categories, and the money variance.
--
-- Drill-down: join back to gold.fact_reconciliation_break (exposed via
-- stg_reconciliation_break) on (business_date, operator_code) to see the
-- individual offending rows.
--
-- Idempotency: replace_where on business_date means re-running any day
-- atomically swaps just that day's rows — Finance can restate a past day
-- with late data without touching an already-closed month.
-- =====================================================================

with population as (
    select * from {{ ref('int_source_population') }}
),

categories as (
    select * from {{ ref('int_break_counts_by_category') }}
)

select
    pop.business_date,
    pop.operator_code,

    -- counts: raw populations (independent) + match outcome (from fact)
    pop.partner_txn_count,
    pop.internal_txn_count,
    coalesce(cat.matched_count, 0)                  as matched_count,
    coalesce(cat.break_count, 0)                    as break_count,
    coalesce(cat.late_arrival_count, 0)             as late_arrival_count,

    -- money: totals carried from the fact + reconciliation variance
    coalesce(cat.partner_amount_total, 0)           as partner_amount_total,
    coalesce(cat.internal_amount_total, 0)          as internal_amount_total,
    coalesce(cat.variance, 0)                       as variance,

    -- break breakdown by the categories named in the brief
    coalesce(cat.amount_mismatch_count, 0)          as amount_mismatch_count,
    coalesce(cat.missing_on_platform_count, 0)      as missing_on_platform_count,
    coalesce(cat.missing_at_partner_count, 0)       as missing_at_partner_count,
    coalesce(cat.orphan_churn_count, 0)             as orphan_churn_count,

    -- a single health ratio for dashboards / alerting
    round(
        coalesce(cat.matched_count, 0)
        / nullif(pop.partner_txn_count, 0)
    , 4)                                            as match_rate,

    current_timestamp()                             as _built_at

from population pop
left join categories cat
    on  pop.business_date = cat.business_date
    and pop.operator_code = cat.operator_code

{% if is_incremental() %}
    -- only (re)build the partitions handed in via the --vars business_date,
    -- or default to everything present in the staged fact on this run.
    where pop.business_date >= (
        select coalesce(min(business_date), '1900-01-01')
        from {{ ref('stg_reconciliation_break') }}
    )
{% endif %}
