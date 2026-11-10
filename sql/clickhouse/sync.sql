-- PostgreSQL -> ClickHouse snapshot loads. Each step fills ${db}.<table>__stage; etl/clickhouse_sync.py
-- creates the stage tables before and swaps them in (EXCHANGE TABLES) after all loads succeeded.
-- Source access goes through the server-side named collection pg_dwh (no credentials in SQL);
-- only the database name is overridden, so tests can point at a throwaway database.
-- ML tables are loaded after the core swap, so they join the already swapped ${db}.dim_city.

-- step: dim_city
INSERT INTO ${db}.dim_city__stage
SELECT gc_id, name, full_name_local, latitude, longitude
FROM postgresql(pg_dwh, database = '${pg_db}', schema = 'core', table = 'dim_city');

-- step: core_prices
INSERT INTO ${db}.core_prices__stage
SELECT price_id, loading_date, loading_city_gcid, unloading_city_gcid, distance, total_weight, total_volume,
       body_group, weight_category, volume_category, price_type, is_fact, complex, organization_id,
       currency_code, price_rub, outlier_mark, etl_run_id
FROM postgresql(pg_dwh, database = '${pg_db}', schema = 'core', table = 'prices');

-- step: core_price_body_groups
INSERT INTO ${db}.core_price_body_groups__stage
SELECT price_id, body_group
FROM postgresql(pg_dwh, database = '${pg_db}', schema = 'core', table = 'price_body_groups');

-- step: core_price_dates
INSERT INTO ${db}.core_price_dates__stage
SELECT price_id, price_date
FROM postgresql(pg_dwh, database = '${pg_db}', schema = 'core', table = 'price_dates');

-- step: ml_price_predictions
INSERT INTO ${db}.ml_price_predictions__stage
SELECT p.price_id, p.model_name, p.split, p.loading_date, p.loading_city_gcid, p.unloading_city_gcid,
       lc.name, uc.name, concat(lc.name, ' → ', uc.name), p.body_group, p.outlier_mark, p.actual_price_rub, p.predicted_price_rub,
       p.abs_error_rub, p.ape_pct, p.etl_run_id, toDateTime(p.scored_at)
FROM postgresql(pg_dwh, database = '${pg_db}', schema = 'mart', table = 'ml_price_predictions') AS p
LEFT JOIN ${db}.dim_city AS lc ON lc.gc_id = p.loading_city_gcid
LEFT JOIN ${db}.dim_city AS uc ON uc.gc_id = p.unloading_city_gcid;

-- step: ml_model_metrics
INSERT INTO ${db}.ml_model_metrics__stage
SELECT etl_run_id, model_name, is_primary, train_start, test_start, test_end, train_rows, test_rows,
       r2, mape_pct, rmse_rub, mae_rub, toDateTime(created_at),
       -- BI convenience flag: Superset does not allow sub-queries in ad-hoc filters
       etl_run_id = max(etl_run_id) OVER () AS is_latest_run
FROM postgresql(pg_dwh, database = '${pg_db}', schema = 'mart', table = 'ml_model_metrics');
