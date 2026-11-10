-- The route card mart computed natively in ClickHouse (MergeTree) from the replicated core tables.
-- Same business rules as sql/mart/market_corridor.sql; parity with PostgreSQL is verified after every load.
-- percentile_cont (PostgreSQL) == quantileExactInclusive (ClickHouse): both interpolate linearly between
-- order statistics. Server-side parameter: {as_of:Date}.

-- step: market_corridor
INSERT INTO ${db}.market_corridor__stage
WITH
deal_months AS (
    -- one row per deal and month; planned months after as_of are excluded
    SELECT price_id, toStartOfMonth(price_date) AS price_month, max(price_date) AS last_price_date
    FROM ${db}.core_price_dates
    WHERE price_date <= {as_of:Date}
    GROUP BY price_id, price_month
),
agg AS (
    SELECT m.price_month AS price_month, p.loading_city_gcid AS loading_city_gcid,
           p.unloading_city_gcid AS unloading_city_gcid, g.body_group AS body_group,
           p.weight_category AS weight_category,
           quantileExactInclusive(0.25)(p.price_rub)              AS p25_trip,
           quantileExactInclusive(0.50)(p.price_rub)              AS median_trip,
           quantileExactInclusive(0.75)(p.price_rub)              AS p75_trip,
           avg(p.price_rub)                                       AS avg_trip,
           quantileExactInclusive(0.25)(p.price_rub / p.distance) AS p25_km,
           quantileExactInclusive(0.50)(p.price_rub / p.distance) AS median_km,
           quantileExactInclusive(0.75)(p.price_rub / p.distance) AS p75_km,
           uniqExact(p.price_id)                                  AS trips_count,
           uniqExactIf(p.organization_id, p.price_type IN (3, 9)) AS carriers_count,
           if(count() < 2 OR avg(p.price_rub) = 0, NULL, stddevSamp(p.price_rub) / avg(p.price_rub)) AS cv_rate,
           max(m.last_price_date)                                 AS last_price_date,
           addMonths(price_month, -1)                             AS prev_month,
           addYears(price_month, -1)                              AS yoy_month
    FROM ${db}.core_prices AS p
    INNER JOIN deal_months AS m ON m.price_id = p.price_id
    INNER JOIN ${db}.core_price_body_groups AS g ON g.price_id = p.price_id
    WHERE p.is_fact AND p.price_type IN (1, 2, 3, 4, 5, 9) AND NOT p.complex AND NOT p.outlier_mark
      AND p.price_rub > 0 AND p.distance > 0
    GROUP BY price_month, loading_city_gcid, unloading_city_gcid, body_group, weight_category
)
SELECT a.price_month, a.loading_city_gcid, a.unloading_city_gcid, lc.name, uc.name,
       concat(lc.name, ' → ', uc.name), a.body_group, a.weight_category,
       a.p25_trip, a.median_trip, a.p75_trip, a.avg_trip, a.p25_km, a.median_km, a.p75_km,
       a.trips_count, a.carriers_count, a.cv_rate,
       -- strictly calendar neighbours (LEFT JOIN with join_use_nulls): missing month -> NULL
       100 * (a.median_trip / nullIf(prev.median_trip, 0) - 1),
       100 * (a.median_trip / nullIf(yoy.median_trip, 0) - 1),
       a.last_price_date,
       dateDiff('day', a.last_price_date, {as_of:Date}),
       multiIf(a.trips_count >= 30 AND a.carriers_count >= 5 AND dateDiff('day', a.last_price_date, {as_of:Date}) <= 30,
               'HIGH', a.trips_count >= 10, 'MEDIUM', 'LOW_DATA'),
       {as_of:Date}
FROM agg AS a
INNER JOIN ${db}.dim_city AS lc ON lc.gc_id = a.loading_city_gcid
INNER JOIN ${db}.dim_city AS uc ON uc.gc_id = a.unloading_city_gcid
LEFT JOIN agg AS prev
       ON prev.price_month = a.prev_month AND prev.loading_city_gcid = a.loading_city_gcid
      AND prev.unloading_city_gcid = a.unloading_city_gcid AND prev.body_group = a.body_group
      AND prev.weight_category = a.weight_category
LEFT JOIN agg AS yoy
       ON yoy.price_month = a.yoy_month AND yoy.loading_city_gcid = a.loading_city_gcid
      AND yoy.unloading_city_gcid = a.unloading_city_gcid AND yoy.body_group = a.body_group
      AND yoy.weight_category = a.weight_category
SETTINGS join_use_nulls = 1;
