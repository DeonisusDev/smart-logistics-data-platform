"""Feature pipeline: pure, deterministic functions without database access (unit tested).

Leakage policy: features are only attributes known when a request is created — route geography,
cargo, body and calendar of the loading date. Anything derived from the target (price, price_rub,
price per km/ton, VAT price, margins, price_group_stats aggregates) and the outlier flag are excluded.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd

TARGET = "price_rub"
UNKNOWN = "Unknown"

CATEGORICAL_FEATURES = [
    "origin_region", "destination_region", "origin_city", "destination_city",
    "body_group", "weight_category", "volume_category", "price_type",
]
NUMERIC_FEATURES = [
    "distance", "log_distance", "total_weight", "total_volume",
    "origin_lat", "origin_lon", "destination_lat", "destination_lon",
    "year", "month", "day_of_week", "day_of_year",
    "month_sin", "month_cos", "dow_sin", "dow_cos", "doy_sin", "doy_cos",
]
FEATURES = CATEGORICAL_FEATURES + NUMERIC_FEATURES

# Never allowed as a feature (target, its derivatives, post-factum aggregates, cleaning flags).
FORBIDDEN_FEATURES = frozenset({
    "price", "price_rub", "price_with_vat", "price_km", "price_per_km", "price_per_ton", "exchange_rate",
    "customer_cost", "transportation_cost", "carrier_cost", "profit",
    "price_group_id", "avg_price", "min_km_price", "max_km_price", "outlier_mark",
})

CYCLES = {"month": 12, "day_of_week": 7, "day_of_year": 365.25}
_CYCLE_PREFIX = {"month": "month", "day_of_week": "dow", "day_of_year": "doy"}


def split_city_region(full_name: str | None) -> tuple[str, str]:
    """'Казань, Республика Татарстан' -> ('Казань', 'Республика Татарстан').

    The region is everything after the first comma; without a comma the region is unknown.
    """
    if full_name is None or (isinstance(full_name, float) and np.isnan(full_name)) or not str(full_name).strip():
        return UNKNOWN, UNKNOWN
    city, sep, region = str(full_name).partition(",")
    return city.strip() or UNKNOWN, (region.strip() if sep and region.strip() else UNKNOWN)


def add_calendar_features(frame: pd.DataFrame, date_column: str = "loading_date") -> pd.DataFrame:
    out = frame.copy()
    dt = pd.to_datetime(out[date_column])
    out["year"] = dt.dt.year
    out["month"] = dt.dt.month
    out["day_of_week"] = dt.dt.dayofweek          # Monday = 0
    out["day_of_year"] = dt.dt.dayofyear
    for column, period in CYCLES.items():
        angle = 2 * np.pi * out[column] / period
        out[f"{_CYCLE_PREFIX[column]}_sin"] = np.sin(angle)
        out[f"{_CYCLE_PREFIX[column]}_cos"] = np.cos(angle)
    return out


def build_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Raw deal rows (core.prices + dim_city columns) -> model matrix with exactly FEATURES columns."""
    out = add_calendar_features(frame)
    for side in ("origin", "destination"):
        parsed = out[f"{side}_full_name"].map(split_city_region)
        out[f"{side}_city"] = parsed.str[0]
        out[f"{side}_region"] = parsed.str[1]
    for column in CATEGORICAL_FEATURES:
        out[column] = out[column].astype("string").fillna(UNKNOWN).astype(str)
    # tariff ~ a * distance + fixed cost: in log-target space log(distance) is close to linear
    out["log_distance"] = np.log1p(pd.to_numeric(out["distance"], errors="coerce").clip(lower=0))
    for column in NUMERIC_FEATURES:
        out[column] = pd.to_numeric(out[column], errors="coerce").astype(float)
    return out[FEATURES]


@dataclass(frozen=True)
class TimeSplit:
    """Chronological split by loading_date relative to the snapshot date (never the system date).

    train = [as_of - 3 months, as_of - 3 weeks), test = [as_of - 3 weeks, as_of].
    """
    as_of: date

    @property
    def train_start(self) -> date:
        return (pd.Timestamp(self.as_of) - pd.DateOffset(months=3)).date()

    @property
    def test_start(self) -> date:
        return self.as_of - timedelta(weeks=3)

    def masks(self, loading_date: pd.Series) -> tuple[pd.Series, pd.Series]:
        d = pd.to_datetime(loading_date).dt.date
        train = (d >= self.train_start) & (d < self.test_start)
        test = (d >= self.test_start) & (d <= self.as_of)
        return train, test
