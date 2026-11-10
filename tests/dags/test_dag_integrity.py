"""DAG integrity: runs inside the Airflow image (docker-compose exec airflow-scheduler pytest /opt/airflow/tests).

Skipped where Airflow is not installed (the pipeline test image).
"""
import os
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS_DIR = os.getenv("DAGS_DIR") or str(Path(__file__).parents[2] / "dags")


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=DAGS_DIR, include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("smart_logistics_rates")


def test_task_order(dag):
    assert [t.task_id for t in dag.topological_sort()] == ["etl_core_mart", "dq_gate", "ml_train_score",
                                                           "clickhouse_sync"]


def test_scheduling_is_safe(dag):
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert all(t.retries >= 0 and t.execution_timeout for t in dag.tasks)
    assert dag.get_task("dq_gate").retries == 0          # a failed quality gate must not be retried blindly


def test_window_and_dq_gate_templates(dag):
    etl = dag.get_task("etl_core_mart").bash_command
    assert "--start-date {{ params.start_date or macros.ds_add(ds, -params.lookback_days) }}" in etl
    assert "--end-date {{ params.end_date or '9999-12-31' }}" in etl
    assert "xcom_pull(task_ids='etl_core_mart')" in dag.get_task("dq_gate").bash_command
