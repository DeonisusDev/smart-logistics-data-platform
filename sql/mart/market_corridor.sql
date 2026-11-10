-- Route card / market rate corridor. Full rebuild in the caller's transaction (idempotent).
-- Grain: month(price_dates.price_date) x origin city x destination city x body group x weight category.
-- volume_category is deliberately not in the grain (task: keep the grain without it), and is not stored
-- either: one cell mixes volume categories, so a single value would be misleading.
-- Parameter: as_of.

-- step: truncate_mart
TRUNCATE mart.market_corridor;

-- step: insert_mart
INSERT INTO mart.market_corridor (
    price_month, loading_city_gcid, unloading_city_gcid, loading_city, unloading_city, body_group,
    weight_category, p25_trip, median_trip, p75_trip, avg_trip, p25_km, median_km, p75_km,
    trips_count, carriers_count, cv_rate, mom_pct, yoy_pct, last_price_date, freshness_days,
    reliability_level, as_of)
WITH params AS (
    SELECT %(as_of)s::date AS as_of
), deal_months AS (
    -- one row per deal and month: a deal must not be weighted twice inside one month;
    -- planned months after as_of never enter the historical statistics
    SELECT d.price_id, date_trunc('month', d.price_date)::date AS price_month, max(d.price_date) AS last_price_date
    FROM core.price_dates d
    CROSS JOIN params
    WHERE d.price_date <= params.as_of
    GROUP BY 1, 2
), base AS (
    SELECT m.price_month, p.loading_city_gcid, p.unloading_city_gcid, g.body_group, p.weight_category,
           p.price_id, p.price_rub, p.price_rub / NULLIF(p.distance, 0) AS price_km,
           p.organization_id, p.price_type, m.last_price_date
    FROM core.prices p
    JOIN deal_months m            ON m.price_id = p.price_id
    JOIN core.price_body_groups g ON g.price_id = p.price_id
    WHERE p.is_fact
      AND p.price_type IN (1, 2, 3, 4, 5, 9)
      AND NOT p.complex
      AND NOT p.outlier_mark
      AND p.price_rub > 0
      AND p.distance > 0
), agg AS (
    SELECT price_month, loading_city_gcid, unloading_city_gcid, body_group, weight_category,
           percentile_cont(0.25) WITHIN GROUP (ORDER BY price_rub) AS p25_trip,
           percentile_cont(0.50) WITHIN GROUP (ORDER BY price_rub) AS median_trip,
           percentile_cont(0.75) WITHIN GROUP (ORDER BY price_rub) AS p75_trip,
           avg(price_rub)                                          AS avg_trip,
           percentile_cont(0.25) WITHIN GROUP (ORDER BY price_km)  AS p25_km,
           percentile_cont(0.50) WITHIN GROUP (ORDER BY price_km)  AS median_km,
           percentile_cont(0.75) WITHIN GROUP (ORDER BY price_km)  AS p75_km,
           count(DISTINCT price_id)                                AS trips_count,
           count(DISTINCT organization_id) FILTER (WHERE price_type IN (3, 9)) AS carriers_count,
           -- stddev_samp is NULL for a single observation
           stddev_samp(price_rub) / NULLIF(avg(price_rub), 0)      AS cv_rate,
           max(last_price_date)                                    AS last_price_date
    FROM base
    GROUP BY 1, 2, 3, 4, 5
)
SELECT a.price_month, a.loading_city_gcid, a.unloading_city_gcid, lc.name, uc.name, a.body_group,
       a.weight_category, a.p25_trip, a.median_trip, a.p75_trip, a.avg_trip, a.p25_km, a.median_km, a.p75_km,
       a.trips_count, a.carriers_count, a.cv_rate,
       -- strictly calendar neighbours: a missing month gives NULL, never the "last available" one
       100 * (a.median_trip / NULLIF(prev.median_trip, 0) - 1) AS mom_pct,
       100 * (a.median_trip / NULLIF(yoy.median_trip, 0) - 1)  AS yoy_pct,
       a.last_price_date,
       params.as_of - a.last_price_date AS freshness_days,
       CASE WHEN a.trips_count >= 30 AND a.carriers_count >= 5 AND params.as_of - a.last_price_date <= 30 THEN 'HIGH'
            WHEN a.trips_count >= 10 THEN 'MEDIUM'
            ELSE 'LOW_DATA' END,
       params.as_of
FROM agg a
CROSS JOIN params
JOIN core.dim_city lc ON lc.gc_id = a.loading_city_gcid
JOIN core.dim_city uc ON uc.gc_id = a.unloading_city_gcid
LEFT JOIN agg prev
       ON prev.price_month = (a.price_month - INTERVAL '1 month')::date
      AND (prev.loading_city_gcid, prev.unloading_city_gcid, prev.body_group, prev.weight_category)
        = (a.loading_city_gcid, a.unloading_city_gcid, a.body_group, a.weight_category)
LEFT JOIN agg yoy
       ON yoy.price_month = (a.price_month - INTERVAL '1 year')::date
      AND (yoy.loading_city_gcid, yoy.unloading_city_gcid, yoy.body_group, yoy.weight_category)
        = (a.loading_city_gcid, a.unloading_city_gcid, a.body_group, a.weight_category);

-- step: analyze_mart
ANALYZE mart.market_corridor;
