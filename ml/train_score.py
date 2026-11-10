"""Time-based training, evaluation and idempotent batch scoring.

    python -m ml.train_score

1. Load deals from core (cleansed, RUB, deduplicated cities, primary body group).
2. Train sample: is_fact, NOT complex, NOT outlier_mark (target cleaning by the source flag only).
3. Chronological split around AS_OF (see ml.features.TimeSplit); every model is fitted on train only.
4. Metrics of the primary model and baselines on test -> log + mart.ml_model_metrics.
5. Batch scoring of the test period and of planned requests (loading_date > AS_OF) with the primary
   model -> mart.ml_price_predictions (DELETE + INSERT of the scored scope in one transaction).
"""
from __future__ import annotations

import logging
import time

import numpy as np
import pandas as pd

from common.config import Settings
from common.db import apply_migrations, connect, job_run
from common.log import log_event, setup_logging
from ml.features import TARGET, TimeSplit, build_features
from ml.models import make_models, regression_metrics

LOG = logging.getLogger("ml.train_score")
ML_LOCK_ID = 26_102_028  # one training/scoring run at a time (pipeline container and Airflow may overlap)

DATASET_SQL = """
SELECT p.price_id, p.loading_date, p.loading_city_gcid, p.unloading_city_gcid,
       p.distance, p.total_weight, p.total_volume, p.body_group, p.weight_category, p.volume_category,
       p.price_type, p.is_fact, p.complex, p.outlier_mark, p.price_rub,
       lc.full_name_local AS origin_full_name, lc.latitude AS origin_lat, lc.longitude AS origin_lon,
       uc.full_name_local AS destination_full_name, uc.latitude AS destination_lat, uc.longitude AS destination_lon
FROM core.prices p
JOIN core.dim_city lc ON lc.gc_id = p.loading_city_gcid
JOIN core.dim_city uc ON uc.gc_id = p.unloading_city_gcid
WHERE p.is_fact AND NOT p.complex AND p.loading_date >= %(train_start)s
ORDER BY p.price_id
"""

PREDICTION_COLUMNS = [
    "price_id", "model_name", "split", "loading_date", "loading_city_gcid", "unloading_city_gcid",
    "body_group", "outlier_mark", "actual_price_rub", "predicted_price_rub", "abs_error_rub", "ape_pct",
    "etl_run_id",
]


def load_dataset(conn, split: TimeSplit) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(DATASET_SQL, {"train_start": split.train_start})
        frame = pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])
    frame["loading_date"] = pd.to_datetime(frame["loading_date"]).dt.date
    return frame


def build_scoring_frame(df: pd.DataFrame, split: TimeSplit, model, model_name: str, run_id: int) -> pd.DataFrame:
    """Test period (facts incl. flagged outliers, for Fact vs Forecast) + planned future requests."""
    _, test_mask = split.masks(df.loading_date)
    future_mask = df.loading_date > split.as_of
    score = df[test_mask | future_mask].copy()
    is_test = score.loading_date <= split.as_of
    score["predicted_price_rub"] = model.predict(build_features(score))
    # Future requests: the price column may be filled in the source, but there is no fact yet.
    score["actual_price_rub"] = score[TARGET].where(is_test)
    score["abs_error_rub"] = (score.actual_price_rub - score.predicted_price_rub).abs()
    score["ape_pct"] = score.abs_error_rub / score.actual_price_rub * 100
    score["split"] = np.where(is_test, "test", "future")
    score["model_name"] = model_name
    score["etl_run_id"] = run_id
    out = score[PREDICTION_COLUMNS].astype(object)
    return out.where(pd.notna(out), None)


def write_predictions(conn, predictions: pd.DataFrame, model_name: str, scope_start) -> int:
    with conn.cursor() as cur:
        # Overwrite the whole scored scope: rows that left the scope since the last run are removed too.
        cur.execute("DELETE FROM mart.ml_price_predictions WHERE model_name = %s AND loading_date >= %s",
                    (model_name, scope_start))
        with cur.copy(f"COPY mart.ml_price_predictions ({', '.join(PREDICTION_COLUMNS)}) FROM STDIN") as copy:
            for row in predictions.itertuples(index=False, name=None):
                copy.write_row(row)
    return len(predictions)


def write_metrics(conn, rows: list[dict]) -> None:
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO mart.ml_model_metrics (etl_run_id, model_name, is_primary, train_start, test_start, test_end,
                                               train_rows, test_rows, r2, mape_pct, rmse_rub, mae_rub)
            VALUES (%(etl_run_id)s, %(model_name)s, %(is_primary)s, %(train_start)s, %(test_start)s, %(test_end)s,
                    %(train_rows)s, %(test_rows)s, %(r2)s, %(mape_pct)s, %(rmse_rub)s, %(mae_rub)s)
        """, rows)


def run(settings: Settings) -> dict:
    apply_migrations(settings.db_dsn)
    split = TimeSplit(settings.as_of)
    params = {"as_of": settings.as_of, "train_start": split.train_start, "test_start": split.test_start,
              "seed": settings.random_seed, "primary_model": settings.model_name}
    log_event(LOG, "ml_started", **params)

    # autocommit: no transaction stays open while models train; the write below is one explicit transaction
    with job_run(settings.db_dsn, "ml_train_score", params) as metrics, \
            connect(settings.db_dsn, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (ML_LOCK_ID,))  # released when the session closes
        run_id = metrics["run_id"]
        df = load_dataset(conn, split)
        train_mask, test_mask = split.masks(df.loading_date)
        clean = ~df.outlier_mark.astype(bool)
        train, test = df[train_mask & clean], df[test_mask & clean]
        log_event(LOG, "dataset_loaded", rows=len(df), train_rows=len(train), test_rows=len(test),
                  future_rows=int((df.loading_date > settings.as_of).sum()))
        if train.empty or test.empty:
            raise RuntimeError(f"Empty time split (train={len(train)}, test={len(test)}); check data and AS_OF")

        x_train, x_test = build_features(train), build_features(test)
        models = make_models(settings.random_seed, settings.catboost_iterations)
        if settings.model_name not in models:
            raise ValueError(f"Unknown MODEL_NAME={settings.model_name}; choose from {sorted(models)}")
        metric_rows, fitted = [], {}
        for name in models:
            started = time.perf_counter()
            model = models[name].fit(x_train, train[TARGET].to_numpy())
            scores = regression_metrics(test[TARGET], model.predict(x_test))
            fitted[name] = model
            metric_rows.append({"etl_run_id": run_id, "model_name": name, "is_primary": name == settings.model_name,
                                "train_start": split.train_start, "test_start": split.test_start,
                                "test_end": settings.as_of, "train_rows": len(train), "test_rows": len(test),
                                **scores})
            log_event(LOG, "model_evaluated", model=name, primary=name == settings.model_name,
                      fit_seconds=round(time.perf_counter() - started, 1), **scores)

        predictions = build_scoring_frame(df, split, fitted[settings.model_name], settings.model_name, run_id)
        with conn.transaction():
            written = write_predictions(conn, predictions, settings.model_name, split.test_start)
            write_metrics(conn, metric_rows)
        n_future = int((predictions.split == "future").sum())
        metrics.update(models={r["model_name"]: {k: r[k] for k in ("r2", "mape_pct", "rmse_rub", "mae_rub")}
                               for r in metric_rows},
                       predictions=written, future_predictions=n_future)
        log_event(LOG, "batch_scored", table="mart.ml_price_predictions", model=settings.model_name,
                  rows=written, test_rows=written - n_future, future_rows=n_future)
    return metrics


def main() -> None:
    setup_logging()
    run(Settings.from_env())


if __name__ == "__main__":
    main()
