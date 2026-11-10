-- Source (public.*) -> core load for one loading_date window.
-- Executed step by step in ONE transaction by etl/pipeline.py; each "-- step:" block is one statement.
-- Parameters: start_date, end_date, run_id.
-- All data-quality rules are defined once, in stage_prices; both core and quarantine read from it.
-- NB: literal percent signs must be doubled in this file (psycopg parameter syntax).

-- step: upsert_dim_city
INSERT INTO core.dim_city (gc_id, city_id, name, full_name_local, latitude, longitude)
SELECT DISTINCT ON (gc_id) gc_id, id, name, full_name_local, latitude, longitude
FROM public.cities
ORDER BY gc_id, id
ON CONFLICT (gc_id) DO UPDATE
SET city_id = EXCLUDED.city_id, name = EXCLUDED.name, full_name_local = EXCLUDED.full_name_local,
    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude;

-- step: upsert_dim_body_type
INSERT INTO core.dim_body_type (body_type_id, name, std_id, body_group)
SELECT id, name, std_id,
       CASE COALESCE(std_id, CASE WHEN id IN (56, 57, 58, 59, 60, 61) THEN 56
                                  WHEN id IN (4, 46, 47, 48) THEN 4
                                  WHEN id = 2 THEN 2
                                  WHEN id IN (1, 15) THEN 15 END)
           WHEN 56 THEN 'Тент'
           WHEN 4  THEN 'Рефрижератор'
           WHEN 2  THEN 'Изотерм'
           WHEN 15 THEN 'Борт'
           ELSE 'Прочие'
       END
FROM public.body_types
ON CONFLICT (body_type_id) DO UPDATE
SET name = EXCLUDED.name, std_id = EXCLUDED.std_id, body_group = EXCLUDED.body_group;

-- step: stage_prices
CREATE TEMP TABLE stg_prices ON COMMIT DROP AS
WITH rates AS (
    -- exact CBR rate per currency and date; guard against duplicated source rows
    SELECT DISTINCT ON (num_code, setting_date) num_code, setting_date, value
    FROM public.exchange_rates
    ORDER BY num_code, setting_date, id DESC
), typed AS (
    SELECT p.id AS price_id, p.created_at, p.loading_date, p.end_loading_date,
           p.loading_city_gcid, p.unloading_city_gcid, p.distance, p.total_weight, p.total_volume,
           p.body_type, p.price, p.currency_code, p.price_type, p.is_fact, p.complex,
           p.organization_id, p.nearest_rate_date, p.outlier_mark,
           -- fill gaps only; the categories delivered by the source are kept as is
           COALESCE(NULLIF(btrim(p.weight_category), ''),
               CASE WHEN round(p.total_weight::numeric, 1) >= 30  THEN 'G20T'
                    WHEN round(p.total_weight::numeric, 1) >= 10  THEN 'FTL'
                    WHEN round(p.total_weight::numeric, 1) >= 5   THEN '10T'
                    WHEN round(p.total_weight::numeric, 1) >= 2.5 THEN '5T'
                    WHEN round(p.total_weight::numeric, 1) >= 0.5 THEN 'L5T'
                    ELSE 'OTH' END) AS weight_category,
           COALESCE(NULLIF(btrim(p.volume_category), ''),
               CASE WHEN p.total_volume > 0 AND p.total_volume <= 120 THEN 'FTL' ELSE 'OTH' END) AS volume_category,
           -- exact rate by the stored nearest_rate_date only (no fallback by loading_date)
           CASE WHEN p.currency_code = '643' THEN 1.0::double precision ELSE r.value END AS exchange_rate,
           lc.gc_id IS NOT NULL AS loading_city_known,
           uc.gc_id IS NOT NULL AS unloading_city_known,
           bt.body_group AS body_type_group,
           pbt.groups AS linked_groups
    FROM public.prices p
    LEFT JOIN rates r            ON r.num_code = p.currency_code AND r.setting_date = p.nearest_rate_date
    LEFT JOIN core.dim_city lc   ON lc.gc_id = p.loading_city_gcid
    LEFT JOIN core.dim_city uc   ON uc.gc_id = p.unloading_city_gcid
    LEFT JOIN core.dim_body_type bt ON bt.body_type_id = p.body_type
    LEFT JOIN LATERAL (
        -- unknown body type codes fall into 'Прочие'; DISTINCT: one row per deal and group
        SELECT array_agg(DISTINCT COALESCE(d.body_group, 'Прочие') ORDER BY COALESCE(d.body_group, 'Прочие')) AS groups
        FROM public.price_body_types x
        LEFT JOIN core.dim_body_type d ON d.body_type_id = x.body_type_id
        WHERE x.price_id = p.id
    ) pbt ON true
    WHERE p.loading_date BETWEEN %(start_date)s AND %(end_date)s
)
SELECT t.*,
       t.price * t.exchange_rate AS price_rub,
       -- primary group: prices.body_type, then the first linked group, then 'Не указан'
       CASE WHEN t.body_type IS NOT NULL THEN COALESCE(t.body_type_group, 'Прочие')
            ELSE COALESCE(t.linked_groups[1], 'Не указан') END AS body_group,
       array_remove(ARRAY[
           CASE WHEN t.price <= 0 THEN 'NON_POSITIVE_PRICE' END,
           CASE WHEN t.currency_code = '643' AND t.price > 0 AND t.price <= 1000 THEN 'RUB_PRICE_OUT_OF_RANGE' END,
           CASE WHEN t.distance <= 0 THEN 'NON_POSITIVE_DISTANCE' END,
           CASE WHEN t.total_weight < 0 THEN 'NEGATIVE_WEIGHT' END,
           CASE WHEN t.total_volume < 0 THEN 'NEGATIVE_VOLUME' END,
           CASE WHEN NOT t.loading_city_known THEN 'UNKNOWN_LOADING_CITY' END,
           CASE WHEN NOT t.unloading_city_known THEN 'UNKNOWN_UNLOADING_CITY' END,
           CASE WHEN t.currency_code NOT IN ('643', '840', '978') THEN 'UNSUPPORTED_CURRENCY' END,
           CASE WHEN t.currency_code IN ('840', '978') AND t.exchange_rate IS NULL THEN 'MISSING_EXACT_RATE' END
       ], NULL) AS reason_codes
FROM typed t;

-- step: index_stage
CREATE INDEX ON stg_prices (price_id);

-- step: delete_core
-- Atomic partition overwrite. Deleting also by price_id handles deals whose loading_date moved into the
-- window; ON DELETE CASCADE removes their bridge and date rows.
DELETE FROM core.prices
WHERE loading_date BETWEEN %(start_date)s AND %(end_date)s
   OR price_id IN (SELECT price_id FROM stg_prices);

-- step: delete_quarantine
DELETE FROM core.dq_quarantine_prices
WHERE loading_date BETWEEN %(start_date)s AND %(end_date)s
   OR price_id IN (SELECT price_id FROM stg_prices);

-- step: insert_core
INSERT INTO core.prices (
    price_id, created_at, loading_date, end_loading_date, loading_city_gcid, unloading_city_gcid,
    distance, total_weight, total_volume, body_type, body_group, weight_category, volume_category,
    price_type, is_fact, complex, organization_id, currency_code, nearest_rate_date, exchange_rate,
    price, price_rub, outlier_mark, etl_run_id)
SELECT price_id, created_at, loading_date, end_loading_date, loading_city_gcid, unloading_city_gcid,
       distance, total_weight, total_volume, body_type, body_group, weight_category, volume_category,
       price_type, is_fact, complex, organization_id, currency_code, nearest_rate_date, exchange_rate,
       price, price_rub, outlier_mark, %(run_id)s
FROM stg_prices
WHERE cardinality(reason_codes) = 0;

-- step: insert_body_groups
-- No price_body_types rows -> fallback to the primary group (prices.body_type or 'Не указан').
INSERT INTO core.price_body_groups (price_id, body_group)
SELECT s.price_id, g.body_group
FROM stg_prices s
CROSS JOIN LATERAL unnest(COALESCE(s.linked_groups, ARRAY[s.body_group])) AS g(body_group)
WHERE cardinality(s.reason_codes) = 0;

-- step: insert_price_dates
INSERT INTO core.price_dates (price_id, price_date)
SELECT d.price_id, d.price_date
FROM public.price_dates d
JOIN stg_prices s ON s.price_id = d.price_id
WHERE cardinality(s.reason_codes) = 0;

-- step: insert_quarantine
INSERT INTO core.dq_quarantine_prices (price_id, loading_date, reason_code, reason_codes, source_row, etl_run_id)
SELECT s.price_id, s.loading_date, s.reason_codes[1], s.reason_codes, to_jsonb(p), %(run_id)s
FROM stg_prices s
JOIN public.prices p ON p.id = s.price_id
WHERE cardinality(s.reason_codes) > 0;
