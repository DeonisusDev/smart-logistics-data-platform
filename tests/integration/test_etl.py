from dataclasses import replace
from datetime import date

import pytest

from etl import pipeline
from tests.integration import seed

VALID_SOURCE_IDS = {r["id"] for r, _ in seed.HAND + seed.ml_sample()} - set(seed.EXPECTED_QUARANTINE)


def test_every_source_row_is_either_loaded_or_quarantined(query):
    core = {r[0] for r in query("SELECT price_id FROM core.prices")}
    quarantine = {r[0] for r in query("SELECT price_id FROM core.dq_quarantine_prices")}
    assert core == VALID_SOURCE_IDS
    assert not core & quarantine
    assert core | quarantine == VALID_SOURCE_IDS | set(seed.EXPECTED_QUARANTINE)


def test_quarantine_reason_codes(query):
    rows = {pid: (code, codes, src) for pid, code, codes, src in
            query("SELECT price_id, reason_code, reason_codes, source_row FROM core.dq_quarantine_prices")}
    assert {pid: code for pid, (code, _, _) in rows.items()} == seed.EXPECTED_QUARANTINE
    # all failed rules are kept, the first one is the primary reason; the raw row is preserved
    assert rows[60][1] == ["NON_POSITIVE_PRICE", "NON_POSITIVE_DISTANCE"]
    assert rows[60][2]["id"] == 60 and rows[60][2]["distance"] == 0


def test_duplicated_city_gc_id_does_not_multiply_deals(query):
    assert query("SELECT count(*), count(DISTINCT price_id) FROM core.prices")[0] == (
        len(VALID_SOURCE_IDS), len(VALID_SOURCE_IDS))
    assert query("SELECT city_id, full_name_local FROM core.dim_city WHERE gc_id = 1002") == [
        (2, "Казань, Республика Татарстан")]


@pytest.mark.parametrize("pid, expected_rub, rate", [(40, 90_000, seed.USD_RATE), (41, 100_000, seed.EUR_RATE),
                                                     (61, 45_000, seed.USD_RATE), (1, 100_000, 1.0)])
def test_currency_conversion_uses_exact_rate(query, pid, expected_rub, rate):
    assert query("SELECT price_rub, exchange_rate FROM core.prices WHERE price_id = %s", (pid,)) == [
        (pytest.approx(expected_rub), pytest.approx(rate))]


def test_missing_categories_are_filled_from_weight_and_volume(query):
    rows = dict((pid, (w, v)) for pid, w, v in query(
        "SELECT price_id, weight_category, volume_category FROM core.prices WHERE price_id IN (42, 43)"))
    assert rows == {42: ("FTL", "FTL"), 43: ("OTH", "OTH")}


def test_body_groups_bridge_and_primary_group(query):
    bridge = {}
    for pid, group in query("SELECT price_id, body_group FROM core.price_body_groups WHERE price_id IN"
                            " (7, 30, 31, 32, 33) ORDER BY 1, 2"):
        bridge.setdefault(pid, []).append(group)
    assert bridge == {7: ["Тент"], 30: ["Борт", "Тент"], 31: ["Не указан"], 32: ["Рефрижератор"], 33: ["Прочие"]}
    primary = dict(query("SELECT price_id, body_group FROM core.prices WHERE price_id IN (30, 31)"))
    assert primary == {30: "Тент", 31: "Не указан"}


def test_rerun_and_backfill_are_idempotent(loaded, query):
    # sums are rounded: float addition order depends on the physical row order after a rewrite
    snapshot_sql = """
        SELECT (SELECT count(*) FROM core.prices), (SELECT round(sum(price_rub)::numeric, 2) FROM core.prices),
               (SELECT count(*) FROM core.price_body_groups), (SELECT count(*) FROM core.price_dates),
               (SELECT count(*) FROM core.dq_quarantine_prices),
               (SELECT count(*) FROM mart.market_corridor), (SELECT round(sum(median_trip)::numeric, 2) FROM mart.market_corridor),
               (SELECT sum(trips_count) FROM mart.market_corridor)"""
    before = query(snapshot_sql)
    pipeline.run(loaded)
    assert query(snapshot_sql) == before
    september = replace(loaded, start_date=date(2026, 9, 1), end_date=date(2026, 9, 30))
    metrics = pipeline.run(september)
    assert metrics["rows_read"] == metrics["rows_loaded"] + metrics["rows_quarantined"]
    assert metrics["rows_replaced"] == metrics["rows_loaded"]
    assert query(snapshot_sql) == before


def test_run_journal_records_success(query):
    statuses = query("SELECT status, metrics ? 'rows_loaded' FROM ops.etl_runs WHERE job = 'etl'")
    assert statuses and all(s == ("success", True) for s in statuses)
