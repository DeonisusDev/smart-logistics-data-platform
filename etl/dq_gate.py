"""Data-quality gate after the ETL load: stops downstream ML and publishing on suspicious loads.

    python -m etl.dq_gate [--run-id N] [--max-quarantine-share 0.01]

Checks the latest successful ETL run (or --run-id) from ops.etl_runs:
  * quarantine share of rows read is below the threshold (a broken source export shows up here first);
  * every loaded deal has price_dates rows (otherwise it silently disappears from the monthly mart);
  * the mart is not empty after a refresh;
  * core and mart are internally consistent: no deal without a body group, no duplicated mart cells.
"""
from __future__ import annotations

import argparse
import logging

from common.config import Settings
from common.db import connect
from common.log import log_event, setup_logging

LOG = logging.getLogger("etl.dq_gate")

CONSISTENCY_CHECKS = {
    "deals_without_body_group": """
        SELECT count(*) FROM core.prices p
        WHERE NOT EXISTS (SELECT 1 FROM core.price_body_groups g WHERE g.price_id = p.price_id)""",
    "duplicated_mart_cells": """
        SELECT count(*) FROM (SELECT 1 FROM mart.market_corridor
            GROUP BY price_month, loading_city_gcid, unloading_city_gcid, body_group, weight_category
            HAVING count(*) > 1) d""",
    "loaded_and_quarantined_at_once": """
        SELECT count(*) FROM core.prices p JOIN core.dq_quarantine_prices q USING (price_id)""",
}


def evaluate(metrics: dict, consistency: dict, max_quarantine_share: float) -> list[str]:
    """Pure decision function (unit tested): returns a list of failed checks."""
    failures = []
    read = metrics.get("rows_read", 0)
    share = metrics.get("rows_quarantined", 0) / read if read else 0.0
    if read == 0:
        failures.append("no rows read in the window")
    if share > max_quarantine_share:
        failures.append(f"quarantine share {share:.4%} > {max_quarantine_share:.4%}")
    if metrics.get("loaded_without_price_dates", 0):
        failures.append(f"{metrics['loaded_without_price_dates']} deals without price_dates")
    if "mart_rows" in metrics and metrics["mart_rows"] == 0:
        failures.append("mart.market_corridor is empty")
    failures += [f"{name} = {value}" for name, value in consistency.items() if value]
    return failures


def run(settings: Settings, run_id: int | None = None, max_quarantine_share: float = 0.01) -> dict:
    with connect(settings.db_dsn) as conn:
        row = conn.execute("""
            SELECT run_id, metrics FROM ops.etl_runs
            WHERE job = 'etl' AND status = 'success' AND (%(run_id)s::bigint IS NULL OR run_id = %(run_id)s)
            ORDER BY run_id DESC LIMIT 1""", {"run_id": run_id}).fetchone()
        if row is None:
            raise RuntimeError("No successful ETL run found in ops.etl_runs")
        checked_run, metrics = row
        consistency = {name: conn.execute(sql).fetchone()[0] for name, sql in CONSISTENCY_CHECKS.items()}
    failures = evaluate(metrics, consistency, max_quarantine_share)
    result = {"etl_run_id": checked_run, "failures": failures, **consistency}
    log_event(LOG, "dq_gate", level=logging.ERROR if failures else logging.INFO, passed=not failures, **result)
    if failures:
        raise RuntimeError(f"DQ gate failed for ETL run {checked_run}: {failures}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--max-quarantine-share", type=float, default=0.01)
    args = parser.parse_args()
    setup_logging()
    run(Settings.from_env(), args.run_id, args.max_quarantine_share)


if __name__ == "__main__":
    main()
