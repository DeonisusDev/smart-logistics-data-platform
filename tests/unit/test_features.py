from datetime import date

import numpy as np
import pandas as pd
import pytest

from ml.features import (CATEGORICAL_FEATURES, FEATURES, FORBIDDEN_FEATURES, UNKNOWN, TimeSplit,
                         add_calendar_features, build_features, split_city_region)


def raw_rows(**overrides) -> pd.DataFrame:
    row = {
        "price_id": 1, "loading_date": date(2026, 10, 1), "distance": 820.0, "total_weight": 20.0,
        "total_volume": 82.0, "body_group": "Тент", "weight_category": "FTL", "volume_category": "FTL",
        "price_type": 3, "origin_full_name": "Москва, г. Москва", "origin_lat": 55.75, "origin_lon": 37.61,
        "destination_full_name": "Казань, Республика Татарстан", "destination_lat": 55.79, "destination_lon": 49.1,
        # target and leaky columns present in the raw frame must never reach the model matrix
        "price_rub": 95_000.0, "price": 95_000.0, "outlier_mark": False,
    }
    row.update(overrides)
    return pd.DataFrame([row])


@pytest.mark.parametrize("full_name, expected", [
    ("Казань, Республика Татарстан", ("Казань", "Республика Татарстан")),
    ("Москва, г. Москва", ("Москва", "г. Москва")),
    ("Ростов-на-Дону, Ростовская обл., Россия", ("Ростов-на-Дону", "Ростовская обл., Россия")),
    ("  Тверь ,  Тверская обл. ", ("Тверь", "Тверская обл.")),
    ("Казань", ("Казань", UNKNOWN)),
    ("Казань,", ("Казань", UNKNOWN)),
    ("", (UNKNOWN, UNKNOWN)),
    (None, (UNKNOWN, UNKNOWN)),
    (float("nan"), (UNKNOWN, UNKNOWN)),
])
def test_split_city_region(full_name, expected):
    assert split_city_region(full_name) == expected


def test_feature_matrix_has_exactly_whitelisted_columns_and_no_leakage():
    x = build_features(raw_rows())
    assert list(x.columns) == FEATURES
    assert not set(x.columns) & FORBIDDEN_FEATURES
    assert x.loc[0, "origin_region"] == "г. Москва"
    assert x.loc[0, "destination_city"] == "Казань"
    assert x.loc[0, "destination_region"] == "Республика Татарстан"


def test_categoricals_are_strings_and_missing_become_unknown():
    x = build_features(raw_rows(body_group=None, origin_full_name=None, price_type=9))
    assert x.loc[0, "body_group"] == UNKNOWN
    assert x.loc[0, "origin_region"] == UNKNOWN
    assert x.loc[0, "price_type"] == "9"
    assert all(x[c].map(type).eq(str).all() for c in CATEGORICAL_FEATURES)


def test_cyclical_encoding_values():
    frame = pd.DataFrame({"loading_date": [date(2026, 12, 31), date(2027, 1, 1), date(2026, 10, 26)]})
    out = add_calendar_features(frame)
    # 2026-10-26 is a Monday -> day_of_week 0 -> sin 0, cos 1
    assert out.loc[2, "day_of_week"] == 0
    assert out.loc[2, "dow_sin"] == pytest.approx(0.0)
    assert out.loc[2, "dow_cos"] == pytest.approx(1.0)
    # December and January are neighbours on the unit circle
    dec, jan = out.loc[0], out.loc[1]
    assert np.hypot(dec.month_sin - jan.month_sin, dec.month_cos - jan.month_cos) == pytest.approx(
        2 * np.sin(np.pi / 12))
    assert np.hypot(dec.doy_sin - jan.doy_sin, dec.doy_cos - jan.doy_cos) < 0.05
    for name in ("month", "dow", "doy"):
        assert np.allclose(out[f"{name}_sin"] ** 2 + out[f"{name}_cos"] ** 2, 1)


def test_time_split_boundaries_match_task():
    split = TimeSplit(date(2026, 10, 26))
    assert split.train_start == date(2026, 7, 26)
    assert split.test_start == date(2026, 10, 5)
    days = pd.Series([date(2026, 7, 25), date(2026, 7, 26), date(2026, 10, 4), date(2026, 10, 5),
                      date(2026, 10, 26), date(2026, 10, 27)])
    train, test = split.masks(days)
    assert train.tolist() == [False, True, True, False, False, False]
    assert test.tolist() == [False, False, False, True, True, False]
    assert not (train & test).any()
