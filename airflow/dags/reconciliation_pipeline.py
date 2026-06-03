"""
Airflow DAG — daily payments reconciliation.

Orchestrates the full medallion flow once per day:

    generate (dev only)
        |
   ┌────┴─────┐
 bronze_op   bronze_internal          (parallel: independent sources)
   └────┬─────┘
        v
   ┌────┴─────────┐
 silver_partner  silver_platform      (parallel: independent transforms)
   └────┬─────────┘
        v
 reconciliation_engine  (gold.fact_reconciliation_break)
        v
 dbt_build              (reconciliation_daily + revenue_monthly_close + tests)
        v
 publish / alert        (notify Finance the morning report is ready)

Key orchestration decisions:
  * `execution_date` drives `business_date`. A backfill of any past day re-runs
    the whole chain for that date and, because every layer is idempotent
    (bronze MERGE, silver/gold replaceWhere, dbt replace_where), the result is a
    clean restatement — no double counting, closed months untouched.
  * Bronze tasks run in parallel; so do the two silver tasks. The engine waits
    for both silver streams.
  * dbt runs AND tests; a failing test fails the DAG so a bad reconciliation
    never silently reaches Finance.

In production each PythonOperator below becomes a DatabricksSubmitRunOperator
(or Databricks Workflow task) pointing at the same modules on the cluster.
The task GRAPH does not change.
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---- config ---------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "depends_on_past": False,   # each day is independent thanks to idempotency
}

# Toggle: in dev we generate synthetic data first; in prod files already land.
GENERATE_SYNTHETIC = True

REPO = "/opt/airflow/repo"   # where the project is mounted on the worker


def _run_module(module: str, arg_flag: str, ds: str):
    """Invoke a pipeline module as `python -m <module> <flag> <ds>`."""
    import subprocess
    import sys

    cmd = [sys.executable, "-m", module, arg_flag, ds]
    subprocess.run(cmd, check=True, cwd=REPO)


with DAG(
    dag_id="tapmad_reconciliation_daily",
    description="Daily partner vs platform payments reconciliation",
    schedule="0 6 * * *",                 # 06:00 UTC, after operator files land
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["reconciliation", "finance", "medallion"],
) as dag:

    # business_date = the logical date this run represents
    ds = "{{ ds }}"

    if GENERATE_SYNTHETIC:
        generate = PythonOperator(
            task_id="generate_synthetic_data",
            python_callable=lambda ds: _run_subprocess_generate(ds),
            op_kwargs={"ds": ds},
        )

    bronze_operator = PythonOperator(
        task_id="bronze_operator_feeds",
        python_callable=_run_module,
        op_kwargs={"module": "spark.ingestion.bronze_operator_feeds",
                   "arg_flag": "--arrival-date", "ds": ds},
    )

    bronze_internal = PythonOperator(
        task_id="bronze_internal_cdc",
        python_callable=_run_module,
        op_kwargs={"module": "spark.ingestion.bronze_internal_cdc",
                   "arg_flag": "--arrival-date", "ds": ds},
    )

    silver_partner = PythonOperator(
        task_id="silver_partner_events",
        python_callable=_run_module,
        op_kwargs={"module": "spark.silver.silver_operator_feeds",
                   "arg_flag": "--business-date", "ds": ds},
    )

    silver_platform = PythonOperator(
        task_id="silver_platform_events",
        python_callable=_run_module,
        op_kwargs={"module": "spark.silver.silver_internal_events",
                   "arg_flag": "--business-date", "ds": ds},
    )

    reconcile = PythonOperator(
        task_id="reconciliation_engine",
        python_callable=_run_module,
        op_kwargs={"module": "spark.gold.reconciliation_engine",
                   "arg_flag": "--business-date", "ds": ds},
    )

    # register the Delta paths as catalog tables so dbt's spark-session target
    # can read silver/gold. (No-op on Databricks where Unity Catalog already
    # holds the tables; needed for the local container metastore.)
    register = PythonOperator(
        task_id="register_tables",
        python_callable=lambda: __import__(
            "subprocess"
        ).run(
            [__import__("sys").executable, "-m", "spark.register_tables"],
            check=True, cwd=REPO,
        ),
    )

    # dbt builds the marts AND runs tests; --vars threads the business_date so
    # the incremental replace_where targets exactly this day.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {REPO}/dbt && "
            "dbt deps && "
            "dbt build "
            "--select staging intermediate marts "
            "--target ${DBT_TARGET:-local} "
            "--vars '{business_date: " + ds
            + ", dbt_incremental_strategy: ${DBT_INC_STRATEGY:-insert_overwrite}}'"
        ),
    )

    # dependency graph
    if GENERATE_SYNTHETIC:
        generate >> [bronze_operator, bronze_internal]
    bronze_operator >> silver_partner
    bronze_internal >> silver_platform
    [silver_partner, silver_platform] >> reconcile >> register >> dbt_build


def _run_subprocess_generate(ds: str):
    import subprocess
    import sys

    subprocess.run(
        [sys.executable, "data/synthetic/generate_data.py",
         "--business-date", ds, "--n", "300"],
        check=True, cwd=REPO,
    )
