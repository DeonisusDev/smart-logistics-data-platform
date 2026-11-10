from datetime import date

import pytest

from ml import train_score
from tests.integration import seed


@pytest.fixture(scope="module")
def scored(loaded):
    first = train_score.run(loaded)
    second = train_score.run(loaded)
    return loaded, first, second


def test_predictions_cover_test_period_and_future_facts_only(scored, query):
    rows = query("SELECT split, min(loading_date), max(loading_date), count(*) FROM mart.ml_price_predictions"
                 " GROUP BY 1 ORDER BY 1")
    (fut, fmin, _, nfut), (test, tmin, tmax, _) = rows
    assert (fut, test) == ("future", "test")
    assert fmin > seed.AS_OF
    assert tmin >= date(2026, 10, 5) and tmax <= seed.AS_OF
    future_bid_or_complex = query("""SELECT count(*) FROM mart.ml_price_predictions m JOIN core.prices p USING (price_id)
                                     WHERE NOT p.is_fact OR p.complex""")
    assert future_bid_or_complex == [(0,)]
    assert nfut == 40


def test_future_rows_have_no_actuals_and_test_rows_have_errors(scored, query):
    assert query("""SELECT count(*) FROM mart.ml_price_predictions WHERE split = 'future'
                    AND (actual_price_rub IS NOT NULL OR abs_error_rub IS NOT NULL OR ape_pct IS NOT NULL)""") == [(0,)]
    assert query("""SELECT bool_and(actual_price_rub > 0 AND abs_error_rub >= 0 AND ape_pct >= 0
                    AND abs(abs_error_rub - abs(actual_price_rub - predicted_price_rub)) < 1e-6)
                    FROM mart.ml_price_predictions WHERE split = 'test'""") == [(True,)]


def test_flagged_outlier_is_scored_for_fact_vs_forecast(scored, query):
    assert query("SELECT count(*) FROM mart.ml_price_predictions WHERE outlier_mark AND split = 'test'") == [(1,)]


def test_scoring_is_idempotent_and_reproducible(scored, query):
    _, first, second = scored
    assert first["predictions"] == second["predictions"]
    assert first["models"] == second["models"]            # fixed seeds -> identical metrics
    assert query("SELECT count(*), count(DISTINCT price_id) FROM mart.ml_price_predictions") == [
        (second["predictions"], second["predictions"])]


def test_metrics_are_stored_for_primary_and_baselines(scored, query):
    _, _, second = scored
    rows = query("SELECT model_name, is_primary, train_start, test_start FROM mart.ml_model_metrics"
                 " WHERE etl_run_id = %s ORDER BY 1", (second["run_id"],))
    assert [r[0] for r in rows] == ["catboost", "hist_gradient_boosting", "ridge"]
    assert [r[1] for r in rows] == [True, False, False]
    assert {(r[2], r[3]) for r in rows} == {(date(2026, 7, 26), date(2026, 10, 5))}
    assert second["models"]["catboost"]["mape_pct"] < 25
