"""Superset configuration: everything sensitive comes from the environment."""
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DATABASE_URI"]

# Simple in-process cache is enough for a single-node demo (no Redis in the stack).
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG

ROW_LIMIT = 50_000
SUPERSET_WEBSERVER_TIMEOUT = 120
BABEL_DEFAULT_LOCALE = "ru"
LANGUAGES = {"ru": {"flag": "ru", "name": "Русский"}, "en": {"flag": "us", "name": "English"}}
