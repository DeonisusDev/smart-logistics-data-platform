import re
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from common.config import SQL_DIR
from common.db import split_steps
from ml.features import build_features
from ml.models import make_catboost, make_hist_gb, make_ridge, regression_metrics


def synthetic_frame(n: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    regions = ["Москва, г. Москва", "Казань, Республика Татарстан", "Самара, Самарская обл."]
    distance = rng.uniform(100, 2000, n)
    return pd.DataFrame({
        "loading_date": [date(2026, 8, 1) + timedelta(days=int(d)) for d in rng.integers(0, 60, n)],
        "distance": distance, "total_weight": rng.uniform(1, 25, n), "total_volume": rng.uniform(10, 120, n),
        "body_group": rng.choice(["Тент", "Рефрижератор"], n), "weight_category": "FTL", "volume_category": "FTL",
        "price_type": rng.choice([1, 3, 9], n),
        "origin_full_name": rng.choice(regions, n), "destination_full_name": rng.choice(regions, n),
        "origin_lat": 55.0, "origin_lon": 40.0, "destination_lat": 54.0, "destination_lon": 45.0,
        "price_rub": 75 * distance + 6000 * rng.lognormal(0, 0.1, n),
    })


@pytest.mark.parametrize("factory", [lambda s: make_catboost(s, iterations=60), make_hist_gb, make_ridge],
                         ids=["catboost", "hist_gb", "ridge"])
def test_models_are_deterministic_and_handle_unseen_categories(factory):
    train = synthetic_frame(300, seed=1)
    unseen = synthetic_frame(5, seed=2).assign(origin_full_name="Новый город, Новая область", body_group="Новый")
    x, x_unseen = build_features(train), build_features(unseen)
    first = factory(42).fit(x, train.price_rub).predict(x_unseen)
    second = factory(42).fit(x, train.price_rub).predict(x_unseen)
    np.testing.assert_allclose(first, second)
    assert np.isfinite(first).all() and (first > 0).all()


def test_regression_metrics():
    m = regression_metrics([100, 200], [110, 180])
    assert m["mape_pct"] == pytest.approx(10.0)
    assert m["mae_rub"] == pytest.approx(15.0)
    assert m["rmse_rub"] == pytest.approx(np.sqrt((100 + 400) / 2))


def test_split_steps_parses_named_statements():
    steps = split_steps("-- header\n-- step: a\nSELECT 1;\n-- step: b_2\nSELECT 2;\n")
    assert steps == [("a", "SELECT 1;"), ("b_2", "SELECT 2;")]


@pytest.mark.parametrize("path", ["etl/load_core.sql", "mart/market_corridor.sql"])
def test_pipeline_sql_has_only_named_parameters(path):
    sql = (SQL_DIR / path).read_text(encoding="utf-8")
    # a bare % would be taken by psycopg as a placeholder and break the statement
    assert not re.findall(r"%(?!\(\w+\)s)", sql.replace("%%", ""))
    assert split_steps(sql)
