# Architecture

End-to-end data flow, medallion layout, orchestration, and the consumer.
The Mermaid diagrams below render directly on GitHub/GitLab.

## Pipeline flow

```mermaid
flowchart TD
    subgraph SRC["Source systems"]
        OPS["6-7 operator feeds<br/>CSV / JSON on SFTP/blob<br/>(different shapes, tz, currency)"]
        OLTP["Internal OLTP via CDC<br/>sub_initial_{op}<br/>sub_recursion_success_{op}<br/>sub_recursion_failure_{op}<br/>user_churn_events"]
    end

    subgraph LAND["Landing (raw files)"]
        L1["landing/operator_feeds/{op}/"]
        L2["landing/internal/{table}_{op}/"]
    end

    subgraph BRONZE["Bronze (raw + metadata, deduped by key)"]
        B1["bronze.operator_feeds<br/>MERGE on (op, partner_txn_id, arrival_date)"]
        B2["bronze.sub_initial / recursion_* / churn<br/>MERGE on PK + operator_code"]
    end

    subgraph SILVER["Silver (canonical, UTC, normalized)"]
        S1["silver.partner_events<br/>7 shapes -> 1 schema, tz->UTC"]
        S2["silver.platform_events<br/>UNION operator-suffixed tables"]
    end

    subgraph GOLD["Gold (reconciliation + marts)"]
        G1["gold.fact_reconciliation_break<br/>(Spark engine: matching decision tree)"]
        G2["reconciliation_daily<br/>(dbt mart)"]
        G3["revenue_monthly_close<br/>(dbt mart)"]
    end

    CONS["Consumers:<br/>Finance morning break report<br/>Monthly book close<br/>Product silent-churn analysis"]

    OPS --> L1 --> B1 --> S1 --> G1
    OLTP --> L2 --> B2 --> S2 --> G1
    G1 --> G2 --> CONS
    G1 --> G3 --> CONS
```

## Orchestration (Airflow DAG)

```mermaid
flowchart LR
    GEN["generate_synthetic_data<br/>(dev only)"] --> BO["bronze_operator_feeds"]
    GEN --> BI["bronze_internal_cdc"]
    BO --> SP["silver_partner_events"]
    BI --> SF["silver_platform_events"]
    SP --> RE["reconciliation_engine<br/>(gold.fact_reconciliation_break)"]
    SF --> RE
    RE --> DBT["dbt build<br/>marts + tests"]
    DBT --> PUB["publish / alert Finance"]
```

## Tooling per layer

| Layer | Engine | Why |
|-------|--------|-----|
| Bronze | PySpark + Delta | schema-on-read flexibility for 7 shapes; MERGE for idempotency |
| Silver | PySpark + Delta | imperative control for tz/format edge cases and the canonical union |
| Gold — fact | PySpark + Delta | window functions + multi-tier matching are clearer/faster in Spark |
| Gold — marts | dbt (Databricks) | SQL Finance can read, tested + documented, incremental replace_where |
| Orchestration | Airflow | dependency graph, retries, backfill = restatement |
| Storage | Delta Lake | ACID, partition replace, time travel for audit |
```
