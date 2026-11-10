-- Warehouse layers on top of the operational source (public.* = raw layer, read-only for the pipeline).
--   ops  : service tables (migrations, job run journal)
--   core : cleansed, typed, deduplicated data at deal grain + conformed dimensions
--   mart : BI-ready aggregates and ML predictions

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE ops.etl_runs (
    run_id      bigserial PRIMARY KEY,
    job         text        NOT NULL,
    status      text        NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed')),
    params      jsonb       NOT NULL DEFAULT '{}',
    metrics     jsonb,
    error       text,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz
);

-- ---------------------------------------------------------------- dimensions
-- cities.gc_id is not unique in the source: keep the row with the minimal id.
CREATE TABLE core.dim_city (  -- upserted, never truncated (referenced by core.prices)
    gc_id           integer PRIMARY KEY,
    city_id         bigint  NOT NULL,
    name            text    NOT NULL,
    full_name_local text    NOT NULL,
    latitude        double precision,
    longitude       double precision
);

-- Standard business body groups (single place where the std_id mapping lives).
CREATE TABLE core.dim_body_type (
    body_type_id integer PRIMARY KEY,
    name         text    NOT NULL,
    std_id       integer,
    body_group   text    NOT NULL
);

-- ---------------------------------------------------------------- facts
-- One row per valid deal. body_group is the *primary* group (from prices.body_type);
-- all compatible groups are in the bridge core.price_body_groups.
CREATE TABLE core.prices (
    price_id            bigint PRIMARY KEY,
    created_at          timestamp,
    loading_date        date             NOT NULL,
    end_loading_date    date,
    loading_city_gcid   integer          NOT NULL REFERENCES core.dim_city (gc_id),
    unloading_city_gcid integer          NOT NULL REFERENCES core.dim_city (gc_id),
    distance            double precision NOT NULL CHECK (distance > 0),
    total_weight        double precision NOT NULL CHECK (total_weight >= 0),
    total_volume        double precision NOT NULL CHECK (total_volume >= 0),
    body_type           integer,
    body_group          text             NOT NULL,
    weight_category     varchar(4)       NOT NULL,
    volume_category     varchar(4)       NOT NULL,
    price_type          smallint         NOT NULL,
    is_fact             boolean          NOT NULL,
    complex             boolean          NOT NULL,
    organization_id     integer,
    currency_code       varchar(3)       NOT NULL,
    nearest_rate_date   date,
    exchange_rate       double precision NOT NULL CHECK (exchange_rate > 0),
    price               double precision NOT NULL CHECK (price > 0),
    price_rub           double precision NOT NULL CHECK (price_rub > 0),
    outlier_mark        boolean          NOT NULL,
    etl_run_id          bigint           NOT NULL,
    loaded_at           timestamptz      NOT NULL DEFAULT now()
);
CREATE INDEX core_prices_loading_date_idx ON core.prices (loading_date);
CREATE INDEX core_prices_route_idx ON core.prices (loading_city_gcid, unloading_city_gcid);

-- Bridge deal x standard body group: a deal with "tent + flatbed" is counted once in each group,
-- never twice inside one group.
CREATE TABLE core.price_body_groups (
    price_id   bigint NOT NULL REFERENCES core.prices (price_id) ON DELETE CASCADE,
    body_group text   NOT NULL,
    PRIMARY KEY (price_id, body_group)
);

-- Monthly unrolling of long-term requests (copy of price_dates for valid deals only).
CREATE TABLE core.price_dates (
    price_id   bigint NOT NULL REFERENCES core.prices (price_id) ON DELETE CASCADE,
    price_date date   NOT NULL,
    PRIMARY KEY (price_id, price_date)
);
CREATE INDEX core_price_dates_date_idx ON core.price_dates (price_date);

-- Rejected source rows with all failed rules and a raw snapshot, so nothing is lost silently.
CREATE TABLE core.dq_quarantine_prices (
    price_id       bigint      PRIMARY KEY,
    loading_date   date        NOT NULL,
    reason_code    text        NOT NULL,
    reason_codes   text[]      NOT NULL,
    source_row     jsonb       NOT NULL,
    etl_run_id     bigint      NOT NULL,
    quarantined_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX dq_quarantine_loading_date_idx ON core.dq_quarantine_prices (loading_date);
CREATE INDEX dq_quarantine_reason_idx ON core.dq_quarantine_prices (reason_code);

-- ---------------------------------------------------------------- marts
-- Grain: month x origin city x destination city x body group x weight category.
CREATE TABLE mart.market_corridor (
    price_month         date    NOT NULL,
    loading_city_gcid   integer NOT NULL,
    unloading_city_gcid integer NOT NULL,
    loading_city        text    NOT NULL,
    unloading_city      text    NOT NULL,
    body_group          text    NOT NULL,
    weight_category     varchar(4) NOT NULL,
    p25_trip            double precision NOT NULL,
    median_trip         double precision NOT NULL,
    p75_trip            double precision NOT NULL,
    avg_trip            double precision NOT NULL,
    p25_km              double precision NOT NULL,
    median_km           double precision NOT NULL,
    p75_km              double precision NOT NULL,
    trips_count         integer NOT NULL,
    carriers_count      integer NOT NULL,
    cv_rate             double precision,
    mom_pct             double precision,
    yoy_pct             double precision,
    last_price_date     date    NOT NULL,
    freshness_days      integer NOT NULL,
    reliability_level   text    NOT NULL CHECK (reliability_level IN ('HIGH', 'MEDIUM', 'LOW_DATA')),
    as_of               date    NOT NULL,
    refreshed_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (price_month, loading_city_gcid, unloading_city_gcid, body_group, weight_category)
);
-- Route card scenario: history of one route/body/weight cell ordered by month.
CREATE INDEX market_corridor_route_idx
    ON mart.market_corridor (loading_city_gcid, unloading_city_gcid, body_group, weight_category, price_month);

CREATE TABLE mart.ml_price_predictions (
    price_id            bigint      NOT NULL,
    model_name          text        NOT NULL,
    split               text        NOT NULL CHECK (split IN ('test', 'future')),
    loading_date        date        NOT NULL,
    loading_city_gcid   integer     NOT NULL,
    unloading_city_gcid integer     NOT NULL,
    body_group          text        NOT NULL,
    outlier_mark        boolean     NOT NULL,
    actual_price_rub    double precision,
    predicted_price_rub double precision NOT NULL,
    abs_error_rub       double precision,
    ape_pct             double precision,
    etl_run_id          bigint      NOT NULL,
    scored_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (price_id, model_name),
    CHECK (split = 'test' OR (actual_price_rub IS NULL AND abs_error_rub IS NULL AND ape_pct IS NULL))
);
CREATE INDEX ml_price_predictions_date_idx ON mart.ml_price_predictions (loading_date);

CREATE TABLE mart.ml_model_metrics (
    etl_run_id   bigint  NOT NULL,
    model_name   text    NOT NULL,
    is_primary   boolean NOT NULL,
    train_start  date    NOT NULL,
    test_start   date    NOT NULL,
    test_end     date    NOT NULL,
    train_rows   integer NOT NULL,
    test_rows    integer NOT NULL,
    r2           double precision NOT NULL,
    mape_pct     double precision NOT NULL,
    rmse_rub     double precision NOT NULL,
    mae_rub      double precision NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (etl_run_id, model_name)
);
