"""Database helpers: connections, migrations and the job run journal (ops.etl_runs)."""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from contextlib import contextmanager
from typing import Iterator

import psycopg

from common.config import SQL_DIR
from common.log import log_event

LOG = logging.getLogger(__name__)
MIGRATIONS_DIR = SQL_DIR / "migrations"
# Arbitrary constant: concurrent jobs (pipeline container, Airflow, tests) never apply migrations twice.
MIGRATION_LOCK_ID = 26_102_026


def connect(dsn: str, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=autocommit, application_name="smart-logistics-dwh")


_STEP_RE = re.compile(r"^--\s*step:\s*(\w+)\s*$", re.MULTILINE)


def split_steps(sql_text: str) -> list[tuple[str, str]]:
    """Split a file into named statements marked with `-- step: <name>` lines."""
    parts = _STEP_RE.split(sql_text)
    steps = [(name, body.strip()) for name, body in zip(parts[1::2], parts[2::2])]
    if not steps:
        raise ValueError("SQL file has no '-- step:' markers")
    return steps


def run_steps(conn: psycopg.Connection, sql_text: str, params: dict, logger: logging.Logger) -> dict[str, int]:
    """Execute named steps sequentially in the caller's transaction; return affected rows per step."""
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for name, statement in split_steps(sql_text):
            started = time.perf_counter()
            cur.execute(statement, params)
            counts[name] = cur.rowcount
            log_event(logger, "sql_step", step=name, rows=cur.rowcount,
                      seconds=round(time.perf_counter() - started, 2))
    return counts


def apply_migrations(dsn: str) -> list[str]:
    """Apply new sql/migrations/*.sql in file-name order, atomically, under an advisory lock."""
    applied: list[str] = []
    with connect(dsn) as conn:
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK_ID,))
            conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ops.schema_migrations (
                    version    text PRIMARY KEY,
                    checksum   text NOT NULL,
                    applied_at timestamptz NOT NULL DEFAULT now()
                )""")
            done = dict(conn.execute("SELECT version, checksum FROM ops.schema_migrations").fetchall())
            for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
                sql = path.read_text(encoding="utf-8")
                checksum = hashlib.sha256(sql.encode()).hexdigest()
                if path.name in done:
                    if done[path.name] != checksum:
                        raise RuntimeError(f"Applied migration {path.name} was modified; add a new migration instead")
                    continue
                with conn.transaction():
                    conn.execute(sql)
                    conn.execute("INSERT INTO ops.schema_migrations (version, checksum) VALUES (%s, %s)",
                                 (path.name, checksum))
                applied.append(path.name)
    log_event(LOG, "migrations_applied", applied=applied)
    return applied


@contextmanager
def job_run(dsn: str, job: str, params: dict) -> Iterator[dict]:
    """Record a job run in ops.etl_runs on a separate autocommit connection.

    The journal survives a rollback of the job's own transaction, so failed runs stay visible.
    The yielded dict collects metrics that are stored on success.
    """
    metrics: dict = {}
    with connect(dsn, autocommit=True) as log_conn:
        run_id = log_conn.execute(
            "INSERT INTO ops.etl_runs (job, params) VALUES (%s, %s) RETURNING run_id",
            (job, json.dumps(params, default=str)),
        ).fetchone()[0]
        metrics["run_id"] = run_id
        try:
            yield metrics
        except Exception as exc:
            log_conn.execute(
                "UPDATE ops.etl_runs SET status = 'failed', finished_at = now(), error = %s WHERE run_id = %s",
                (f"{type(exc).__name__}: {exc}"[:2000], run_id),
            )
            raise
        log_conn.execute(
            "UPDATE ops.etl_runs SET status = 'success', finished_at = now(), metrics = %s WHERE run_id = %s",
            (json.dumps({k: v for k, v in metrics.items() if k != "run_id"}, default=str), run_id),
        )
