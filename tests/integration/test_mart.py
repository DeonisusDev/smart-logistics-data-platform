from datetime import date

import pytest

CELL = "loading_city_gcid = %s AND unloading_city_gcid = %s AND body_group = %s AND weight_category = 'FTL'"


def cell(query, month, lc=1001, uc=1002, body="Тент"):
    rows = query(f"""SELECT trips_count, carriers_count, median_trip, p25_trip, p75_trip, avg_trip, median_km,
                            cv_rate, mom_pct, yoy_pct, reliability_level, loading_city, unloading_city
                     FROM mart.market_corridor WHERE price_month = %s AND {CELL}""", (month, lc, uc, body))
    return dict(zip(["trips", "carriers", "p50", "p25", "p75", "avg", "p50_km", "cv", "mom", "yoy", "level",
                     "from", "to"], rows[0])) if rows else None


def test_corridor_metrics_exclude_outliers_complex_and_bids(query):
    sep = cell(query, date(2026, 9, 1))
    # valid deals 100k, 110k, 120k, 105k; outlier 400k, complex 50k and bid 60k are excluded
    assert sep["trips"] == 4
    assert sep["carriers"] == 2           # organisations of price_type 3/9 only: 701 (twice) and 702
    assert sep["p50"] == pytest.approx(107_500)
    assert sep["p25"] == pytest.approx(103_750)
    assert sep["p75"] == pytest.approx(112_500)
    assert sep["avg"] == pytest.approx(108_750)
    assert sep["p50_km"] == pytest.approx(107.5)
    assert (sep["from"], sep["to"]) == ("Москва", "Казань")


def test_mom_and_yoy_use_calendar_neighbours_only(query):
    sep, aug = cell(query, date(2026, 9, 1)), cell(query, date(2026, 8, 1))
    assert sep["mom"] == pytest.approx(7.5)                        # vs Aug 100k
    assert sep["yoy"] == pytest.approx(100 * (107_500 / 88_000 - 1))
    assert aug["mom"] is None                                      # July missing: no fallback to June
    assert aug["yoy"] is None
    assert aug["cv"] is None                                       # single observation


def test_long_term_request_counted_once_per_month_and_not_in_future(query):
    months = [r[0] for r in query(f"SELECT price_month FROM mart.market_corridor WHERE {CELL} ORDER BY 1",
                                  (1001, 1003, "Тент"))]
    assert months == [date(2026, 8, 1), date(2026, 9, 1), date(2026, 10, 1)]
    assert query("SELECT count(*) FROM mart.market_corridor WHERE price_month > '2026-10-26'") == [(0,)]


def test_deal_with_two_body_groups_is_in_both_cells(query):
    for body in ("Тент", "Борт"):
        assert cell(query, date(2026, 9, 1), 1002, 1003, body)["trips"] == 1


def test_reliability_levels(query):
    assert cell(query, date(2026, 10, 1), 1003, 1001)["level"] == "HIGH"
    assert cell(query, date(2026, 3, 1), 1003, 1001)["level"] == "MEDIUM"   # 12 trips, not fresh
    assert cell(query, date(2026, 9, 1))["level"] == "LOW_DATA"


def test_mart_grain_is_unique(query):
    assert query("""SELECT count(*) FROM (SELECT 1 FROM mart.market_corridor
                    GROUP BY price_month, loading_city_gcid, unloading_city_gcid, body_group, weight_category
                    HAVING count(*) > 1) d""") == [(0,)]
