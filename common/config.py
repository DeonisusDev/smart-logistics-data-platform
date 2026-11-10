"""Runtime settings read from environment variables (see .env.example)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_ROOT / "sql"

# Wide defaults: the source contains planned loadings after AS_OF (until 2027-02),
# and they must reach Core to be scored by the ML job.
DEFAULT_START = date(1900, 1, 1)
DEFAULT_END = date(9999, 12, 31)


def _env_date(name: str, default: date) -> date:
    raw = os.getenv(name, "").strip()
    return date.fromisoformat(raw) if raw else default


@dataclass(frozen=True)
class Settings:
    db_dsn: str
    as_of: date
    start_date: date
    end_date: date
    random_seed: int = 42
    model_name: str = "catboost"
    catboost_iterations: int = 800
    # ClickHouse analytical layer (bonus); the sync is skipped when clickhouse_host is empty.
    clickhouse_host: str = ""
    clickhouse_port: int = 8123
    clickhouse_user: str = "analytics"
    clickhouse_password: str = ""
    clickhouse_database: str = "analytics"

    def __post_init__(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError(f"START_DATE {self.start_date} is after END_DATE {self.end_date}")

    @classmethod
    def from_env(cls, **overrides) -> "Settings":
        dsn = os.getenv("DB_DSN")
        if not dsn and "db_dsn" not in overrides:
            raise RuntimeError("DB_DSN is not set; copy .env.example to .env or export DB_DSN")
        values = dict(
            db_dsn=dsn,
            as_of=_env_date("AS_OF", date(2026, 10, 26)),
            start_date=_env_date("START_DATE", DEFAULT_START),
            end_date=_env_date("END_DATE", DEFAULT_END),
            random_seed=int(os.getenv("RANDOM_SEED", "42")),
            model_name=os.getenv("MODEL_NAME", "catboost"),
            clickhouse_host=os.getenv("CLICKHOUSE_HOST", ""),
            clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            clickhouse_user=os.getenv("CLICKHOUSE_USER", "analytics"),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            clickhouse_database=os.getenv("CLICKHOUSE_DATABASE", "analytics"),
        )
        values.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**values)


def read_sql(relative_path: str) -> str:
    return (SQL_DIR / relative_path).read_text(encoding="utf-8")
