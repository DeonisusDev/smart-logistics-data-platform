import pytest

from etl.clickhouse_sync import _ident, parity_sql
from etl.dq_gate import evaluate

GOOD = {"rows_read": 1000, "rows_quarantined": 5, "loaded_without_price_dates": 0, "mart_rows": 10}
CLEAN = {"deals_without_body_group": 0, "duplicated_mart_cells": 0, "loaded_and_quarantined_at_once": 0}


def test_dq_gate_passes_normal_load():
    assert evaluate(GOOD, CLEAN, max_quarantine_share=0.01) == []


@pytest.mark.parametrize("metrics, consistency, expected", [
    ({**GOOD, "rows_quarantined": 50}, CLEAN, "quarantine share"),
    ({**GOOD, "rows_read": 0, "rows_quarantined": 0}, CLEAN, "no rows read"),
    ({**GOOD, "loaded_without_price_dates": 3}, CLEAN, "without price_dates"),
    ({**GOOD, "mart_rows": 0}, CLEAN, "is empty"),
    (GOOD, {**CLEAN, "duplicated_mart_cells": 2}, "duplicated_mart_cells = 2"),
])
def test_dq_gate_failures(metrics, consistency, expected):
    failures = evaluate(metrics, consistency, max_quarantine_share=0.01)
    assert len(failures) == 1 and expected in failures[0]


def test_core_only_run_without_mart_is_not_flagged():
    metrics = {k: v for k, v in GOOD.items() if k != "mart_rows"}
    assert evaluate(metrics, CLEAN, 0.01) == []


@pytest.mark.parametrize("bad", ["analytics; DROP TABLE x", "a-b", "1abc", "", "a b"])
def test_clickhouse_identifiers_are_validated(bad):
    with pytest.raises(ValueError):
        _ident(bad)


def test_parity_sql_compares_every_mart_metric():
    sql = parity_sql("analytics", "deals_bigdata")
    for column in ("median_trip", "p75_km", "trips_count", "carriers_count", "mom_pct", "yoy_pct", "cv_rate",
                   "reliability_level", "freshness_days"):
        assert f"c.{column}" in sql
    assert "postgresql(pg_dwh, database = 'deals_bigdata', schema = 'mart'" in sql
