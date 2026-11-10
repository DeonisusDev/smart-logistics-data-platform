"""ETL: source (public.*) -> core -> mart, idempotent and parameterised by a loading_date window.

    python -m etl.pipeline                                   # whole source, window from env
    python -m etl.pipeline --start-date 2026-09-01 --end-date 2026-09-30   # backfill

Core load, quarantine and mart refresh run in a single transaction: a rerun of the same window
overwrites it atomically (DELETE + INSERT), so counts, sums and percentiles never double.
"""
from __future__ import annotations

import argparse
import logging
from datetime import date

from common.config import Settings, read_sql
from common.db import apply_migrations, connect, job_run, run_steps
from common.log import log_event, setup_logging

LOG = logging.getLogger("etl.pipeline")
ETL_LOCK_ID = 26_102_027  # one ETL run at a time


def run(settings: Settings, refresh_mart: bool = True) -> dict:
    apply_migrations(settings.db_dsn)
    window = {"start_date": settings.start_date, "end_date": settings.end_date}
    params = {**window, "as_of": settings.as_of}
    log_event(LOG, "etl_started", **params)

    with job_run(settings.db_dsn, "etl", params) as metrics, connect(settings.db_dsn) as conn:
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (ETL_LOCK_ID,))
            steps = run_steps(conn, read_sql("etl/load_core.sql"), {**window, "run_id": metrics["run_id"]}, LOG)
            quarantine = dict(conn.execute(
                "SELECT reason_code, count(*) FROM core.dq_quarantine_prices"
                " WHERE etl_run_id = %s GROUP BY 1 ORDER BY 1", (metrics["run_id"],)).fetchall())
            no_dates = conn.execute(
                "SELECT count(*) FROM core.prices p WHERE p.etl_run_id = %s"
                " AND NOT EXISTS (SELECT 1 FROM core.price_dates d WHERE d.price_id = p.price_id)",
                (metrics["run_id"],)).fetchone()[0]
            metrics.update(
                rows_read=steps["stage_prices"],
                rows_loaded=steps["insert_core"],
                rows_quarantined=steps["insert_quarantine"],
                quarantine_by_reason=quarantine,
                body_group_rows=steps["insert_body_groups"],
                price_date_rows=steps["insert_price_dates"],
                rows_replaced=steps["delete_core"],
                loaded_without_price_dates=no_dates,
            )
            log_event(LOG, "core_loaded", **window, **{k: v for k, v in metrics.items() if k != "run_id"})
            if no_dates:
                log_event(LOG, "dq_warning", logging.WARNING, check="loaded_without_price_dates",
                          rows=no_dates, impact="deals are absent from the monthly mart")

            if refresh_mart:
                conn.execute("SET LOCAL work_mem = '64MB'")
                mart_steps = run_steps(conn, read_sql("mart/market_corridor.sql"), {"as_of": settings.as_of}, LOG)
                reliability = dict(conn.execute(
                    "SELECT reliability_level, count(*) FROM mart.market_corridor GROUP BY 1 ORDER BY 1").fetchall())
                metrics.update(mart_rows=mart_steps["insert_mart"], mart_reliability=reliability)
                log_event(LOG, "mart_refreshed", table="mart.market_corridor", as_of=settings.as_of,
                          rows=mart_steps["insert_mart"], reliability=reliability)
    log_event(LOG, "etl_finished", run_id=metrics["run_id"])
    return metrics


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-date", type=date.fromisoformat, help="window start (loading_date), inclusive")
    parser.add_argument("--end-date", type=date.fromisoformat, help="window end (loading_date), inclusive")
    parser.add_argument("--as-of", type=date.fromisoformat, help="snapshot date for the mart")
    parser.add_argument("--skip-mart", action="store_true", help="load core only")
    args = parser.parse_args(argv)
    setup_logging()
    settings = Settings.from_env(start_date=args.start_date, end_date=args.end_date, as_of=args.as_of)
    metrics = run(settings, refresh_mart=not args.skip_mart)
    # Last stdout line = run id: Airflow's BashOperator pushes it to XCom for the downstream DQ gate.
    print(metrics["run_id"], flush=True)


if __name__ == "__main__":
    main()
