-- ClickHouse analytical layer (bonus). ${db} is substituted by etl/clickhouse_sync.py (validated identifier).
--
-- Every table is a full snapshot of its PostgreSQL source: it is loaded into a __stage copy and swapped in
-- with EXCHANGE TABLES (atomic). This is idempotent by construction and also removes rows that disappeared
-- from the source - which ReplacingMergeTree cannot do without FINAL and tombstones. At this data volume
-- (hundreds of thousands of rows) a full swap takes seconds.

-- step: create_database
CREATE DATABASE IF NOT EXISTS ${db};

-- step: create_dim_city
CREATE TABLE IF NOT EXISTS ${db}.dim_city (
    gc_id           UInt32,
    name            String,
    full_name_local String,
    latitude        Nullable(Float64),
    longitude       Nullable(Float64)
) ENGINE = MergeTree ORDER BY gc_id;

-- step: create_core_prices
CREATE TABLE IF NOT EXISTS ${db}.core_prices (
    price_id            UInt64,
    loading_date        Date,
    loading_city_gcid   UInt32,
    unloading_city_gcid UInt32,
    distance            Float64,
    total_weight        Float64,
    total_volume        Float64,
    body_group          LowCardinality(String),
    weight_category     LowCardinality(String),
    volume_category     LowCardinality(String),
    price_type          UInt8,
    is_fact             Bool,
    complex             Bool,
    organization_id     Nullable(Int32),
    currency_code       LowCardinality(String),
    price_rub           Float64,
    outlier_mark        Bool,
    etl_run_id          UInt64
) ENGINE = MergeTree PARTITION BY toYYYYMM(loading_date) ORDER BY (loading_city_gcid, unloading_city_gcid, price_id);

-- step: create_core_price_body_groups
CREATE TABLE IF NOT EXISTS ${db}.core_price_body_groups (
    price_id   UInt64,
    body_group LowCardinality(String)
) ENGINE = MergeTree ORDER BY (price_id, body_group);

-- step: create_core_price_dates
CREATE TABLE IF NOT EXISTS ${db}.core_price_dates (
    price_id   UInt64,
    price_date Date
) ENGINE = MergeTree ORDER BY (price_id, price_date);

-- step: create_market_corridor
-- Computed inside ClickHouse from the replicated core tables, then checked against the PostgreSQL mart.
CREATE TABLE IF NOT EXISTS ${db}.market_corridor (
    price_month         Date,
    loading_city_gcid   UInt32,
    unloading_city_gcid UInt32,
    loading_city        LowCardinality(String),
    unloading_city      LowCardinality(String),
    route               String,
    body_group          LowCardinality(String),
    weight_category     LowCardinality(String),
    p25_trip            Float64,
    median_trip         Float64,
    p75_trip            Float64,
    avg_trip            Float64,
    p25_km              Float64,
    median_km           Float64,
    p75_km              Float64,
    trips_count         UInt32,
    carriers_count      UInt32,
    cv_rate             Nullable(Float64),
    mom_pct             Nullable(Float64),
    yoy_pct             Nullable(Float64),
    last_price_date     Date,
    freshness_days      Int32,
    reliability_level   LowCardinality(String),
    as_of               Date
) ENGINE = MergeTree
PARTITION BY toYear(price_month)
ORDER BY (loading_city_gcid, unloading_city_gcid, body_group, weight_category, price_month);

-- step: create_ml_price_predictions
CREATE TABLE IF NOT EXISTS ${db}.ml_price_predictions (
    price_id            UInt64,
    model_name          LowCardinality(String),
    split               LowCardinality(String),
    loading_date        Date,
    loading_city_gcid   UInt32,
    unloading_city_gcid UInt32,
    loading_city        LowCardinality(String),
    unloading_city      LowCardinality(String),
    route               String,
    body_group          LowCardinality(String),
    outlier_mark        Bool,
    actual_price_rub    Nullable(Float64),
    predicted_price_rub Float64,
    abs_error_rub       Nullable(Float64),
    ape_pct             Nullable(Float64),
    etl_run_id          UInt64,
    scored_at           DateTime
) ENGINE = MergeTree PARTITION BY toYYYYMM(loading_date) ORDER BY (model_name, loading_date, price_id);

-- step: create_ml_model_metrics
CREATE TABLE IF NOT EXISTS ${db}.ml_model_metrics (
    etl_run_id  UInt64,
    model_name  LowCardinality(String),
    is_primary  Bool,
    train_start Date,
    test_start  Date,
    test_end    Date,
    train_rows  UInt32,
    test_rows   UInt32,
    r2          Float64,
    mape_pct    Float64,
    rmse_rub    Float64,
    mae_rub     Float64,
    created_at  DateTime,
    is_latest_run Bool
) ENGINE = MergeTree ORDER BY (etl_run_id, model_name);

-- step: evolve_ml_model_metrics
-- additive schema evolution for tables created by an earlier version
ALTER TABLE ${db}.ml_model_metrics ADD COLUMN IF NOT EXISTS is_latest_run Bool;
