"""Structured logging: one event per line, `text` (key=value) or `json` format.

Usage: log_event(LOG, "core_loaded", rows=123, window="2026-09-01..2026-09-30")
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime

_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime"}


def _fields(record: logging.LogRecord) -> dict:
    return {k: v for k, v in vars(record).items() if k not in _RESERVED}


def _jsonable(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float):
        return round(value, 6)
    return value


class KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = f"{self.formatTime(record)} {record.levelname} {record.name} event={record.getMessage()}"
        extra = " ".join(f"{k}={json.dumps(_jsonable(v), ensure_ascii=False, default=str)}"
                         for k, v in _fields(record).items())
        out = f"{base} {extra}".rstrip()
        if record.exc_info:
            out += "\n" + self.formatException(record.exc_info)
        return out


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {"ts": self.formatTime(record), "level": record.levelname, "logger": record.name,
                   "event": record.getMessage(), **{k: _jsonable(v) for k, v in _fields(record).items()}}
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if os.getenv("LOG_FORMAT", "text") == "json" else KeyValueFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(os.getenv("LOG_LEVEL", "INFO"))


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields) -> None:
    logger.log(level, event, extra=fields)
