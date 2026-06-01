# Azure Migration Plan

> **Context for the reviewer:** I built this on the stack I run today —
> open-source Spark + Delta, dbt, Airflow — so it runs on a laptop and the
> engineering is easy to inspect. Tapmad runs **PySpark notebooks on Azure
> Fabric with ADLS Gen2**. This document is the concrete, from-scratch plan to
> move what's in this repo onto Azure. The good news: because every layer is
> already Spark + Delta + idempotent partition writes, the **business logic
> does not change** — only *where it runs* and *where data lives*.

---

## 1. Target Azure architecture

```
Operators (SFTP)            Internal OLTP
      |                          |
      v                          v
  Azure Data Factory  <----  ADF / Fabric Dataflow (CDC)
   (SFTP -> ADLS)                 |
      |                           |
      +-------------+-------------+
                    v
        ADLS Gen2  (abfss://lake@acct.dfs.core.windows.net)
          /landing  /bronze  /silver  /gold     <-- Delta tables
                    |
                    v
     Azure Databricks / Microsoft Fabric Spark
       (the spark/ jobs run unchanged as notebooks/jobs)
                    |
                    v
        dbt-databricks  (marts: reconciliation_daily, revenue_monthly_close)
                    |
                    v
   Unity Catalog  ->  Power BI / Fabric reports for Finance & Product
```

**Component mapping**

| This repo (local) | Azure equivalent |
|-------------------|------------------|
| `lakehouse/` folders | ADLS Gen2 container with `landing/bronze/silver/gold` paths |
| local Spark session | Azure Databricks cluster **or** Fabric Spark notebook |
| `spark/.../*.py` jobs | same files as Databricks Jobs / Fabric notebooks (no code change) |
| Delta tables on disk | Delta tables on ADLS, registered in **Unity Catalog** |
| Airflow DAG | **Databricks Workflows** or **ADF pipelines** (or keep Airflow via Azure-managed Airflow in ADF) |
| dbt-core local run | **dbt-databricks** against the same cluster/SQL warehouse |
| SFTP landing (manual) | **ADF Copy activity** polling operator SFTP into `landing/` |

---

## 2. Step-by-step migration (from scratch)

### Phase 0 — Foundations (1 day)
1. Create a **resource group**, an **ADLS Gen2 storage account** (hierarchical
   namespace ON), and one container `lake`.
2. Create folders: `landing/`, `bronze/`, `silver/`, `gold/`.
3. Stand up **Azure Databricks** (or a **Fabric** workspace + Lakehouse).
4. Enable **Unity Catalog**; create catalog `tapmad`, schemas
   `bronze/silver/gold/staging/marts`.

### Phase 1 — Repoint storage paths (½ day)
Only **one file changes**: `spark/config/paths.py`. Swap local paths for
`abfss://` URIs:
```python
LAKEHOUSE_ROOT = "abfss://lake@<account>.dfs.core.windows.net"
BRONZE_OPERATOR_FEEDS = f"{LAKEHOUSE_ROOT}/bronze/operator_feeds"
# ...etc
```
Register each Delta path as a Unity Catalog table once:
```sql
CREATE TABLE tapmad.bronze.operator_feeds
USING DELTA LOCATION 'abfss://lake@.../bronze/operator_feeds';
```
> Tip: switch the Spark jobs from `.save(path)` to `.saveAsTable("tapmad.bronze.operator_feeds")`
> so Unity Catalog governs them directly. `delta_utils.py` is the only module
> to touch — change `merge_upsert` / `overwrite_partition` to write by table
> name. Logic is identical.

### Phase 2 — Land the data (1–2 days)
- **Operator feeds:** ADF pipeline with an **SFTP linked service** per operator
  → Copy activity → `landing/operator_feeds/{operator}/`. Schedule hourly.
- **Internal OLTP:** enable CDC and use ADF / Fabric Dataflow Gen2 (or Debezium
  → Event Hubs → Spark structured streaming) to land CDC JSON into
  `landing/internal/`. The bronze MERGE already handles re-delivered changes.

### Phase 3 — Run the Spark jobs on Azure (1 day)
- Each `spark/.../*.py` becomes a **Databricks Job task** (or a Fabric
  notebook). `get_spark()` already returns the cluster's existing session, so
  no edits.
- Parameterize `--business-date` / `--arrival-date` from the job's run
  parameters (Databricks `dbutils.widgets` or ADF pipeline parameters).

### Phase 4 — dbt on Databricks (½ day)
- `dbt/profiles.yml` already targets `type: databricks`. Fill in the Azure
  Databricks `host`, `http_path` (SQL warehouse), and a token / OAuth.
- `dbt build` runs the marts + tests unchanged. `replace_where` incremental
  strategy is natively supported by `dbt-databricks` on Delta.

### Phase 5 — Orchestration (1 day)
Two clean options:
- **Databricks Workflows (recommended):** recreate the DAG graph in
  `airflow/dags/reconciliation_pipeline.py` as a multi-task Workflow. Same
  dependency edges; Databricks handles retries/backfill.
- **Keep Airflow:** use **Managed Airflow in ADF** and swap each
  `PythonOperator` for `DatabricksSubmitRunOperator`. The graph is identical;
  only the operator type changes.

### Phase 6 — Serve & govern (½ day)
- Expose `tapmad.marts.reconciliation_daily` and `revenue_monthly_close` to
  **Power BI** (DirectQuery on the SQL warehouse) for Finance.
- Use **Unity Catalog** grants for row/column governance; the data is financial.

---

## 3. What carries over with zero change
- All matching logic in `reconciliation_engine.py` (it's pure Spark DataFrame).
- All silver normalization (tz/format handling is plain Spark).
- All dbt models, tests, and the `replace_where` idempotency strategy.
- The Airflow task graph shape.

## 4. What must change (and it's small)
| Change | File(s) | Effort |
|--------|---------|--------|
| Storage paths → `abfss://` / `saveAsTable` | `spark/config/paths.py`, `spark/utils/delta_utils.py` | low |
| dbt connection details | `dbt/profiles.yml` | trivial |
| Orchestrator operators | `airflow/dags/...` → Workflows/ADF | low–medium |
| Ingestion (file landing + CDC) | new ADF pipelines (replaces local generator) | medium |

## 5. Azure-specific things I'd add for production
- **Auto Loader** (`cloudFiles`) for bronze ingestion — incremental, schema
  evolution, exactly-once file tracking — instead of `glob`.
- **Delta Live Tables** as an alternative to hand-rolled bronze/silver if the
  team prefers declarative pipelines with built-in data-quality expectations.
- **Liquid clustering / Z-ORDER** on `business_date, operator_code` for the
  fact table at scale.
- **Unity Catalog lineage** so Finance can trace a number to source rows in the
  UI (complements the `partner_txn_id` audit columns I already carry).
- **Key Vault** for SFTP creds and the Databricks/dbt token.
- **Cost control:** job clusters (not all-purpose) + autoscaling + spot for the
  nightly batch.
