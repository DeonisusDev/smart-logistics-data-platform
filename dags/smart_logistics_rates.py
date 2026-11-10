"""
### Smart Logistics: daily rates pipeline

`etl_core_mart` → `dq_gate` → `ml_train_score` → `clickhouse_sync`

* **Window.** By default the run reloads deals with `loading_date` in
  `[logical_date − lookback_days, +∞)`: late-arriving deals for recent days are picked up, and planned
  loadings after the snapshot reach Core for scoring. Each step is idempotent, so retries and reruns are safe.
* **Backfill.** *Trigger DAG w/ config*: `{"start_date": "2026-09-01", "end_date": "2026-09-30"}`,
  or `airflow dags backfill smart_logistics_rates -s 2026-09-01 -e 2026-09-30`.
* **as_of.** The snapshot date of the mart and the ML split. Defaults to `AS_OF` from the environment
  (fixed `2026-10-26` for the test dataset); set the `as_of` param to `""` to use the end of the data interval.
* Tasks run the same CLI entrypoints as `docker-compose up` (`etl/`, `ml/`), in an isolated virtualenv,
  so Airflow's own dependencies never clash with the pipeline's.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PIPELINE_DIR = os.getenv("PIPELINE_DIR", "/opt/pipeline")
PYTHON = f"{PIPELINE_DIR}/venv/bin/python"

WINDOW_START = "{{ params.start_date or macros.ds_add(ds, -params.lookback_days) }}"
WINDOW_END = "{{ params.end_date or '9999-12-31' }}"
AS_OF = "{{ params.as_of or data_interval_end | ds }}"

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="smart_logistics_rates",
    description="ETL -> DQ gate -> ML batch scoring -> ClickHouse",
    doc_md=__doc__,
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 10, 27, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "start_date": Param(None, type=["null", "string"], format="date", description="window start, inclusive"),
        "end_date": Param(None, type=["null", "string"], format="date", description="window end, inclusive"),
        "lookback_days": Param(30, type="integer", minimum=0, description="default window: logical_date minus N days"),
        "as_of": Param(os.getenv("AS_OF", ""), type="string", description="snapshot date; empty = data_interval_end"),
        "max_quarantine_share": Param(0.01, type="number", minimum=0, maximum=1),
    },
    tags=["smart-logistics", "etl", "ml", "clickhouse"],
) as dag:
    common = {"cwd": PIPELINE_DIR, "append_env": True, "env": {"AS_OF": AS_OF}}

    etl = BashOperator(
        task_id="etl_core_mart",
        doc_md="Source → core (DQ, quarantine, RUB) → mart.market_corridor, one transaction.",
        bash_command=f"{PYTHON} -m etl.pipeline --start-date {WINDOW_START} --end-date {WINDOW_END} --as-of {AS_OF}",
        **common,
    )
    dq_gate = BashOperator(
        task_id="dq_gate",
        doc_md="Blocks ML and publishing when the load looks broken (quarantine share, consistency).",
        bash_command=(f"{PYTHON} -m etl.dq_gate --run-id {{{{ ti.xcom_pull(task_ids='etl_core_mart') }}}}"
                      " --max-quarantine-share {{ params.max_quarantine_share }}"),
        retries=0,
        **common,
    )
    ml = BashOperator(
        task_id="ml_train_score",
        doc_md="Time-based train/test, metrics, batch scoring into mart.ml_price_predictions.",
        bash_command=f"{PYTHON} -m ml.train_score",
        **common,
    )
    clickhouse = BashOperator(
        task_id="clickhouse_sync",
        doc_md="Snapshot to ClickHouse, native MergeTree mart, row-level parity check with PostgreSQL.",
        bash_command=f"{PYTHON} -m etl.clickhouse_sync",
        **common,
    )

    etl >> dq_gate >> ml >> clickhouse
