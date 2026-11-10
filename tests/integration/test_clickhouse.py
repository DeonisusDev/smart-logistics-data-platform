"""ClickHouse bonus layer: snapshot sync, native mart and parity with PostgreSQL.

Needs a ClickHouse server whose named collection pg_dwh points at the same PostgreSQL server as
TEST_DB_DSN (true inside docker-compose). Skipped when ClickHouse is unreachable.
"""
import os
import uuid
from dataclasses import replace
from datetime import date

import pytest
from psycopg.conninfo import conninfo_to_dict

from etl import clickhouse_sync

CH_HOST = os.getenv("TEST_CLICKHOUSE_HOST") or os.getenv("CLICKHOUSE_HOST", "")


@pytest.fixture(scope="module")
def ch(loaded):
    if not CH_HOST:
        pytest.skip("ClickHouse is not configured (TEST_CLICKHOUSE_HOST / CLICKHOUSE_HOST)")
    settings = replace(loaded, clickhouse_host=CH_HOST,
                       clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
                       clickhouse_user=os.getenv("CLICKHOUSE_USER", "analytics"),
                       clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                       clickhouse_database=f"analytics_test_{uuid.uuid4().hex[:10]}")
    try:
        client = clickhouse_sync.get_client(settings)
    except Exception as exc:  # noqa: BLE001 - any connection problem means "not available"
        pytest.skip(f"ClickHouse is not reachable: {exc.__class__.__name__}")
    pg_db = conninfo_to_dict(settings.db_dsn)["dbname"]
    first = clickhouse_sync.run(settings, pg_database=pg_db)
    second = clickhouse_sync.run(settings, pg_database=pg_db)
    yield client, settings.clickhouse_database, first, second
    client.command(f"DROP DATABASE IF EXISTS {settings.clickhouse_database}")
    client.close()


def test_parity_with_postgres_mart(ch, query):
    _, _, first, _ = ch
    parity = first["parity"]
    assert parity["ok"], parity
    assert parity["clickhouse_rows"] == query("SELECT count(*) FROM mart.market_corridor")[0][0] > 0


def test_snapshot_row_counts_match_postgres(ch, query):
    _, _, first, _ = ch
    assert first["rows"]["core_prices"] == query("SELECT count(*) FROM core.prices")[0][0]
    assert first["rows"]["core_price_body_groups"] == query("SELECT count(*) FROM core.price_body_groups")[0][0]
    assert first["rows"]["dim_city"] == query("SELECT count(*) FROM core.dim_city")[0][0]


def test_hand_checked_cell_in_clickhouse(ch):
    client, db, _, _ = ch
    rows = client.query(f"""
        SELECT price_month, trips_count, carriers_count, median_trip, mom_pct, cv_rate, route
        FROM {db}.market_corridor
        WHERE loading_city_gcid = 1001 AND unloading_city_gcid = 1002 AND body_group = 'Тент'
          AND weight_category = 'FTL' AND price_month IN ('2026-08-01', '2026-09-01')
        ORDER BY price_month""").result_rows
    aug, sep = rows
    assert sep[:4] == (date(2026, 9, 1), 4, 2, pytest.approx(107_500))
    assert sep[4] == pytest.approx(7.5)
    assert sep[6] == "Москва → Казань"
    assert aug[4] is None and aug[5] is None       # missing July -> NULL MoM; single deal -> NULL cv


def test_resync_is_idempotent(ch):
    _, _, first, second = ch
    assert first["rows"] == second["rows"]
    assert second["parity"]["ok"]
