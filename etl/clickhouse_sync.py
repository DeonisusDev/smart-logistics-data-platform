"""Bonus: ClickHouse analytical layer.

    python -m etl.clickhouse_sync

1. Snapshot core/mart tables from PostgreSQL into ClickHouse (postgresql() table function, executed by
   the ClickHouse server - no data passes through Python).
2. Rebuild the route card mart natively in ClickHouse (MergeTree) from the replicated core tables.
3. Parity check: the ClickHouse mart must match mart.market_corridor in PostgreSQL row by row; the job
   fails otherwise. This is the guarantee that the bonus layer gives the same results as the base one.

Every table is loaded into <table>__stage and swapped in with EXCHANGE TABLES, so reruns are idempotent
and readers (Superset) never see a half-loaded table.
"""
from __future__ import annotations

import logging
import re
import time
from string import Template

import clickhouse_connect
from psycopg.conninfo import conninfo_to_dict

from common.config import Settings, read_sql
from common.db import job_run, split_steps
from common.log import log_event, setup_logging

LOG = logging.getLogger("etl.clickhouse_sync")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

CORE_TABLES = ["dim_city", "core_prices", "core_price_body_groups", "core_price_dates"]
ML_TABLES = ["ml_price_predictions", "ml_model_metrics"]
MART_TABLE = "market_corridor"
MART_KEYS = ["price_month", "loading_city_gcid", "unloading_city_gcid", "body_group", "weight_category"]
MART_EXACT = ["loading_city", "unloading_city", "trips_count", "carriers_count", "last_price_date",
              "freshness_days", "reliability_level", "as_of"]
MART_FLOAT = ["p25_trip", "median_trip", "p75_trip", "avg_trip", "p25_km", "median_km", "p75_km"]
MART_NULLABLE_FLOAT = ["cv_rate", "mom_pct", "yoy_pct"]
REL_TOLERANCE = 1e-9


def _ident(value: str) -> str:
    if not _IDENT.match(value):
        raise ValueError(f"Unsafe identifier: {value!r}")
    return value


def get_client(settings: Settings):
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port, username=settings.clickhouse_user,
        password=settings.clickhouse_password, connect_timeout=10, send_receive_timeout=600)


def _render(sql: str, db: str, pg_db: str) -> str:
    return Template(sql).substitute(db=db, pg_db=pg_db)


def _run_file(client, path: str, db: str, pg_db: str, params: dict | None = None, only: list[str] | None = None):
    for name, statement in split_steps(_render(read_sql(path), db, pg_db)):
        if only is not None and name not in only:
            continue
        started = time.perf_counter()
        client.command(statement, parameters=params)
        log_event(LOG, "clickhouse_step", file=path, step=name, seconds=round(time.perf_counter() - started, 2))


def _load_and_swap(client, tables: list[str], path: str, db: str, pg_db: str, params: dict | None = None) -> dict:
    """Fill <table>__stage for all tables first, then swap them in. Returns row counts."""
    for table in tables:
        client.command(f"CREATE OR REPLACE TABLE {db}.{table}__stage AS {db}.{table}")
    _run_file(client, path, db, pg_db, params, only=tables)
    counts = {}
    for table in tables:
        counts[table] = client.command(f"SELECT count() FROM {db}.{table}__stage")
        client.command(f"EXCHANGE TABLES {db}.{table}__stage AND {db}.{table}")
        client.command(f"DROP TABLE {db}.{table}__stage")
    return counts


def parity_sql(db: str, pg_db: str) -> str:
    """Row-level comparison of the ClickHouse mart with the PostgreSQL mart (executed in ClickHouse)."""
    def close(col: str) -> str:
        return f"abs(c.{col} - p.{col}) <= {REL_TOLERANCE} * greatest(abs(c.{col}), abs(p.{col}), 1)"

    checks = [f"c.{col} = p.{col}" for col in MART_EXACT]
    checks += [close(col) for col in MART_FLOAT]
    checks += [f"((c.{col} IS NULL AND p.{col} IS NULL) OR ({close(col)}))" for col in MART_NULLABLE_FLOAT]
    on = " AND ".join(f"c.{k} = p.{k}" for k in MART_KEYS)
    pg = f"postgresql(pg_dwh, database = '{pg_db}', schema = 'mart', table = '{MART_TABLE}')"
    return f"""
        SELECT (SELECT count() FROM {db}.{MART_TABLE}) AS clickhouse_rows,
               (SELECT count() FROM {pg}) AS postgres_rows,
               count() AS matched_rows,
               countIf(NOT ({' AND '.join(checks)})) AS mismatched_rows
        FROM {db}.{MART_TABLE} AS c
        INNER JOIN {pg} AS p ON {on}
    """


def check_parity(client, db: str, pg_db: str) -> dict:
    row = client.query(parity_sql(db, pg_db)).first_row
    result = dict(zip(["clickhouse_rows", "postgres_rows", "matched_rows", "mismatched_rows"], row))
    result["ok"] = (result["clickhouse_rows"] == result["postgres_rows"] == result["matched_rows"]
                    and result["mismatched_rows"] == 0)
    return result


def run(settings: Settings, pg_database: str | None = None) -> dict:
    if not settings.clickhouse_host:
        log_event(LOG, "clickhouse_sync_skipped", reason="CLICKHOUSE_HOST is not set")
        return {"skipped": True}
    db = _ident(settings.clickhouse_database)
    pg_db = _ident(pg_database or conninfo_to_dict(settings.db_dsn)["dbname"])
    params = {"clickhouse_database": db, "postgres_database": pg_db, "as_of": settings.as_of}
    log_event(LOG, "clickhouse_sync_started", **params)

    with job_run(settings.db_dsn, "clickhouse_sync", params) as metrics:
        client = get_client(settings)
        try:
            _run_file(client, "clickhouse/schema.sql", db, pg_db)
            rows = _load_and_swap(client, CORE_TABLES, "clickhouse/sync.sql", db, pg_db)
            rows |= _load_and_swap(client, [MART_TABLE], "clickhouse/market_corridor.sql", db, pg_db,
                                   {"as_of": settings.as_of})
            rows |= _load_and_swap(client, ML_TABLES, "clickhouse/sync.sql", db, pg_db)
            parity = check_parity(client, db, pg_db)
        finally:
            client.close()
        metrics.update(rows=rows, parity=parity)
        log_event(LOG, "clickhouse_synced", rows=rows)
        log_event(LOG, "clickhouse_parity", level=logging.INFO if parity["ok"] else logging.ERROR, **parity)
        if not parity["ok"]:
            raise RuntimeError(f"ClickHouse mart differs from PostgreSQL mart: {parity}")
    return metrics


def main() -> None:
    setup_logging()
    run(Settings.from_env())


if __name__ == "__main__":
    main()
