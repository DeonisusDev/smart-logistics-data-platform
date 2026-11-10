"""Integration fixtures: a throwaway database per test session on the configured PostgreSQL server.

Server DSN: TEST_DB_DSN, else DB_DSN, else local default. Tests are skipped if the server is unreachable.
The database is created from sql/schema.sql + tests/integration/seed.py and dropped afterwards.
"""
from __future__ import annotations

import os
import uuid

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from common.config import DEFAULT_END, DEFAULT_START, SQL_DIR, Settings
from etl import pipeline
from tests.integration import seed

SERVER_DSN = os.getenv("TEST_DB_DSN") or os.getenv("DB_DSN") or "postgresql://postgres:postgres@localhost:5432/postgres"


def pytest_collection_modifyitems(items):
    for item in items:
        if "integration" in str(item.path):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def database():
    try:
        admin = psycopg.connect(SERVER_DSN, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL is not reachable ({exc.__class__.__name__}); set TEST_DB_DSN to run integration tests")
    name = f"etl_test_{uuid.uuid4().hex[:10]}"
    admin.execute(f'CREATE DATABASE "{name}"')
    dsn = make_conninfo(SERVER_DSN, dbname=name)
    try:
        with psycopg.connect(dsn) as conn:
            conn.execute((SQL_DIR / "schema.sql").read_text(encoding="utf-8"))
            seed.load_source(conn)
        yield dsn
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()


@pytest.fixture(scope="session")
def settings(database) -> Settings:
    return Settings(db_dsn=database, as_of=seed.AS_OF, start_date=DEFAULT_START, end_date=DEFAULT_END,
                    catboost_iterations=60)


@pytest.fixture(scope="session")
def loaded(settings) -> Settings:
    """Database after one full ETL run."""
    pipeline.run(settings)
    return settings


@pytest.fixture
def query(loaded):
    def run(sql: str, params=None):
        with psycopg.connect(loaded.db_dsn) as conn:
            return conn.execute(sql, params).fetchall()
    return run
