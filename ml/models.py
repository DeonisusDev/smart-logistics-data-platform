"""Model definitions. Every preprocessing step lives inside an sklearn Pipeline, so imputers,
encoders and scalers are fitted on the train split only (no temporal leakage).

The target is modelled as log1p(price_rub): tariffs are log-normal, the loss then penalises relative
errors (what MAPE measures) instead of being dominated by long-haul trips. Metrics are in rubles.
"""
from __future__ import annotations

import numpy as np
from catboost import CatBoostRegressor
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler

from ml.features import CATEGORICAL_FEATURES, NUMERIC_FEATURES

PRIMARY_MODEL = "catboost"


def _log_target(regressor) -> TransformedTargetRegressor:
    return TransformedTargetRegressor(regressor=regressor, func=np.log1p, inverse_func=np.expm1)


def make_ridge(seed: int) -> TransformedTargetRegressor:
    pre = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=20), CATEGORICAL_FEATURES),
        ("num", Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]),
         NUMERIC_FEATURES),
    ])
    return _log_target(Pipeline([("pre", pre), ("model", Ridge(alpha=1.0, random_state=seed))]))


def make_hist_gb(seed: int) -> TransformedTargetRegressor:
    pre = ColumnTransformer([
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=np.nan,
                               encoded_missing_value=np.nan), CATEGORICAL_FEATURES),
        ("num", "passthrough", NUMERIC_FEATURES),  # NaN is handled natively by the model
    ], verbose_feature_names_out=False)
    model = HistGradientBoostingRegressor(
        max_iter=400, learning_rate=0.06, max_leaf_nodes=31, l2_regularization=1.0,
        categorical_features=list(range(len(CATEGORICAL_FEATURES))), early_stopping=False, random_state=seed)
    return _log_target(Pipeline([("pre", pre), ("model", model)]))


def make_catboost(seed: int, iterations: int = 800) -> TransformedTargetRegressor:
    model = CatBoostRegressor(
        iterations=iterations, learning_rate=0.08, depth=6, loss_function="RMSE",
        cat_features=CATEGORICAL_FEATURES, random_seed=seed, thread_count=4,
        verbose=False, allow_writing_files=False)
    return _log_target(model)


def make_models(seed: int, catboost_iterations: int = 800) -> dict:
    """Primary model first; the others are baselines reported for comparison only.

    The primary model is fixed up front and not chosen by test metrics (that would leak the test set
    into model selection).
    """
    return {
        PRIMARY_MODEL: make_catboost(seed, catboost_iterations),
        "hist_gradient_boosting": make_hist_gb(seed),
        "ridge": make_ridge(seed),
    }


def regression_metrics(y_true, y_pred) -> dict[str, float]:
    y_true, y_pred = np.asarray(y_true, dtype=float), np.asarray(y_pred, dtype=float)
    return {
        "r2": float(r2_score(y_true, y_pred)),
        "mape_pct": float(mean_absolute_percentage_error(y_true, y_pred) * 100),
        "rmse_rub": float(mean_squared_error(y_true, y_pred) ** 0.5),
        "mae_rub": float(mean_absolute_error(y_true, y_pred)),
    }
