"""Superset dashboard as code: database, datasets, charts and dashboard, imported with overwrite.

    python build_dashboard.py

Dataset columns are read from ClickHouse system.columns, so datasets always match the real schema.
UUIDs are deterministic (uuid5), so every run updates the same objects instead of duplicating them.
`superset import-dashboards` overwrites only the dashboard itself (charts/datasets keep their old
definition), so each asset level is imported here with its own command and overwrite=True.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import clickhouse_connect
import yaml

NS = uuid.UUID("5d1c5e4e-2026-4c26-9b0a-6d3a1b7e0001")
CH_DB = os.environ.get("CLICKHOUSE_DATABASE", "analytics")
DB_NAME = "ClickHouse Analytics"
# Superset forbids sub-queries in ad-hoc SQL (ALLOW_ADHOC_SUBQUERY is off, and should stay off), so
# "latest" is expressed through columns of the mart: the month of the snapshot date / the latest run flag.
LAST_MONTH = "price_month = toStartOfMonth(as_of)"


def uid(name: str) -> str:
    return str(uuid.uuid5(NS, name))


def metric(label: str, sql: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label, "hasCustomLabel": True,
            "optionName": f"metric_{uid(label + sql)[:8]}"}


def where(sql: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "clause": "WHERE", "subject": None, "operator": None,
            "comparator": None, "isExtra": False, "isNew": False, "filterOptionName": f"filter_{uid(sql)[:8]}"}


# --------------------------------------------------------------------------- charts
def big_number(title, dataset, label, sql, filters=(), fmt=",.0f", subheader=""):
    return dict(title=title, dataset=dataset, viz_type="big_number_total", params={
        "metric": metric(label, sql), "adhoc_filters": [where(f) for f in filters],
        "y_axis_format": fmt, "subheader": subheader, "header_font_size": 0.4, "subheader_font_size": 0.15})


def timeseries(title, dataset, x_axis, grain, metrics, viz="echarts_timeseries_line", fmt=",.0f",
               filters=(), description=None):
    return dict(title=title, dataset=dataset, viz_type=viz, description=description, params={
        "x_axis": x_axis, "time_grain_sqla": grain, "x_axis_sort_asc": True, "metrics": metrics,
        "groupby": [], "adhoc_filters": [where(f) for f in filters], "row_limit": 10000,
        "truncate_metric": True, "show_legend": True, "legendType": "scroll", "legendOrientation": "top",
        "rich_tooltip": True, "tooltipTimeFormat": "smart_date", "y_axis_format": fmt,
        "x_axis_time_format": "smart_date", "markerEnabled": True, "markerSize": 4, "zoomable": True,
        "orientation": "vertical", "only_total": True, "comparison_type": "values", "annotation_layers": [],
        "seriesType": "line", "sort_series_type": "sum"})


def raw_table(title, dataset, columns, order_by, filters=(), row_limit=500, formats=None, description=None):
    return dict(title=title, dataset=dataset, viz_type="table", description=description, params={
        "query_mode": "raw", "all_columns": columns, "order_by_cols": [f'["{c}", {str(asc).lower()}]'
                                                                       for c, asc in order_by],
        "adhoc_filters": [where(f) for f in filters], "row_limit": row_limit, "server_pagination": False,
        "table_timestamp_format": "%Y-%m-%d", "include_search": True, "show_cell_bars": False,
        "allow_rearrange_columns": True, "color_pn": True,
        "column_config": {c: {"d3NumberFormat": f} for c, f in (formats or {}).items()}})


def pie(title, dataset, groupby, label, sql, filters=()):
    return dict(title=title, dataset=dataset, viz_type="pie", params={
        "groupby": [groupby], "metric": metric(label, sql), "adhoc_filters": [where(f) for f in filters],
        "row_limit": 100, "sort_by_metric": True, "donut": True, "show_labels": True, "label_type": "key_percent",
        "show_legend": True, "legendOrientation": "top", "number_format": ",.0f", "innerRadius": 40,
        "outerRadius": 70})


MC, PR, MM = "market_corridor", "ml_price_predictions", "ml_model_metrics"
MONEY = ",.0f"

MARKET = [
    [big_number("Ячеек рынка за последний месяц", MC, "Ячеек", "count()", [LAST_MONTH],
                subheader="маршрут × кузов × вес"),
     big_number("Ячеек с надёжностью HIGH", MC, "HIGH", "countIf(reliability_level = 'HIGH')", [LAST_MONTH],
                subheader="≥30 рейсов, ≥5 перевозчиков, свежесть ≤30 дн."),
     big_number("Медиана ставки, руб./рейс", MC, "P50", "median(median_trip)", [LAST_MONTH],
                subheader="медиана P50 по выбранным ячейкам"),
     big_number("Медиана ставки, руб./км", MC, "P50 км", "median(median_km)", [LAST_MONTH], fmt=",.1f",
                subheader="медиана P50 по выбранным ячейкам")],
    [timeseries("Коридор ставок P25–P50–P75, руб./рейс", MC, "price_month", "P1M",
                [metric("P25", "avg(p25_trip)"), metric("P50", "avg(median_trip)"), metric("P75", "avg(p75_trip)")],
                description="Выберите маршрут, группу кузова и весовую категорию в фильтрах — "
                            "получится детальная карточка одного направления."),
     timeseries("Коридор ставок P25–P50–P75, руб./км", MC, "price_month", "P1M",
                [metric("P25 км", "avg(p25_km)"), metric("P50 км", "avg(median_km)"),
                 metric("P75 км", "avg(p75_km)")], fmt=",.1f")],
    [timeseries("Доказательная база: рейсы и перевозчики", MC, "price_month", "P1M",
                [metric("Рейсов (сумма по ячейкам)", "sum(trips_count)"),
                 metric("Перевозчиков (сумма по ячейкам)", "sum(carriers_count)")],
                viz="echarts_timeseries_bar"),
     # by cells LOW_DATA is ~99%; by trips it shows how much of the market has a reliable benchmark
     pie("Доля рейсов по уровню надёжности (последний месяц)", MC, "reliability_level", "Рейсов",
         "sum(trips_count)", [LAST_MONTH])],
    [raw_table("Сводка маршрутной сети за последний месяц", MC,
               ["route", "body_group", "weight_category", "p25_trip", "median_trip", "p75_trip", "median_km",
                "trips_count", "carriers_count", "cv_rate", "mom_pct", "yoy_pct", "reliability_level"],
               [("trips_count", False)], [LAST_MONTH],
               formats={"p25_trip": MONEY, "median_trip": MONEY, "p75_trip": MONEY, "median_km": ",.1f",
                        "cv_rate": ".3f", "mom_pct": "+.1f", "yoy_pct": "+.1f"})],
]
ML = [
    [big_number("MAPE на test, %", PR, "MAPE", "avg(ape_pct)", ["split = 'test'", "NOT outlier_mark"],
                fmt=".2f", subheader="очищенная выборка 2026-10-05…2026-10-26"),
     big_number("MAE на test, руб.", PR, "MAE", "avg(abs_error_rub)", ["split = 'test'", "NOT outlier_mark"]),
     big_number("Оценено будущих заявок", PR, "Заявок", "count()", ["split = 'future'"],
                subheader="loading_date > as_of, факт = NULL")],
    [timeseries("Факт vs Прогноз по дням погрузки, руб./рейс", PR, "loading_date", "P1D",
                [metric("Факт (среднее)", "avg(actual_price_rub)"),
                 metric("Прогноз (среднее)", "avg(predicted_price_rub)")],
                description="Справа от 2026-10-26 — прогноз для плановых заявок без факта.")],
    [raw_table("Рейсы, закрытые дороже прогноза более чем на 30%", PR,
               ["price_id", "loading_date", "route", "body_group", "actual_price_rub", "predicted_price_rub",
                "ape_pct", "outlier_mark"],
               [("ape_pct", False)], ["split = 'test'", "actual_price_rub > 1.3 * predicted_price_rub"],
               formats={"actual_price_rub": MONEY, "predicted_price_rub": MONEY, "ape_pct": ".1f"})],
]
MODEL = [
    [raw_table("Метрики моделей последнего запуска (test)", MM,
               ["model_name", "is_primary", "train_rows", "test_rows", "r2", "mape_pct", "rmse_rub", "mae_rub"],
               [("is_primary", False), ("mape_pct", True)],
               ["is_latest_run"],
               formats={"r2": ".3f", "mape_pct": ".2f", "rmse_rub": MONEY, "mae_rub": MONEY},
               description="Основная модель (CatBoost) выбрана заранее; бейзлайны — для сравнения.")],
]
TABS = [("TAB-market", "Рыночный коридор", MARKET), ("TAB-ml", "Факт vs Прогноз", ML),
        ("TAB-model", "Качество модели", MODEL)]
# Chart identity = its slot in the layout, so renaming a chart updates it instead of creating a new one.
for _tab_id, _, _rows in TABS:
    for _r, _row in enumerate(_rows):
        for _c, _chart in enumerate(_row):
            _chart["uuid"] = uid(f"chart:{_tab_id}:{_r}:{_c}")

FILTERS = [  # (id, name, dataset, column, tab scope)
    ("NATIVE_FILTER-route", "Маршрут", MC, "route", "TAB-market"),
    ("NATIVE_FILTER-body", "Группа кузова", MC, "body_group", "TAB-market"),
    ("NATIVE_FILTER-weight", "Весовая категория", MC, "weight_category", "TAB-market"),
    ("NATIVE_FILTER-ml-route", "Маршрут (ML)", PR, "route", "TAB-ml"),
    ("NATIVE_FILTER-ml-body", "Группа кузова (ML)", PR, "body_group", "TAB-ml"),
]


# --------------------------------------------------------------------------- bundle
def dataset_yaml(client, table: str, db_uuid: str) -> dict:
    rows = client.query("SELECT name, type FROM system.columns WHERE database = {db:String} AND table = {t:String}"
                        " ORDER BY position", parameters={"db": CH_DB, "t": table}).result_rows
    if not rows:
        raise RuntimeError(f"{CH_DB}.{table} not found in ClickHouse - run the pipeline first")
    temporal = [n for n, t in rows if t.startswith(("Date", "Nullable(Date"))]
    return {
        "table_name": table, "main_dttm_col": temporal[0] if temporal else None, "description": None,
        "default_endpoint": None, "offset": 0, "cache_timeout": None, "schema": CH_DB, "sql": None, "params": None,
        "template_params": None, "filter_select_enabled": True, "fetch_values_predicate": None, "extra": None,
        "normalize_columns": False, "always_filter_main_dttm": False, "uuid": uid(f"dataset:{table}"),
        "metrics": [{"metric_name": "count", "verbose_name": "COUNT(*)", "metric_type": "count",
                     "expression": "count()", "description": None, "d3format": None, "extra": {},
                     "warning_text": None}],
        "columns": [{"column_name": n, "verbose_name": None, "is_dttm": n in temporal, "is_active": True,
                     "type": t, "advanced_data_type": None, "groupby": True, "filterable": True,
                     "expression": None, "description": None, "python_date_format": None, "extra": {}}
                    for n, t in rows],
        "version": "1.0.0", "database_uuid": db_uuid,
    }


def chart_yaml(chart: dict) -> dict:
    return {"slice_name": chart["title"], "description": chart.get("description"), "certified_by": None,
            "certification_details": None, "viz_type": chart["viz_type"],
            "params": {"viz_type": chart["viz_type"], **chart["params"]}, "query_context": None,
            "cache_timeout": None, "uuid": chart["uuid"], "version": "1.0.0",
            "dataset_uuid": uid(f"dataset:{chart['dataset']}")}


def position_json() -> dict:
    pos = {"DASHBOARD_VERSION_KEY": "v2",
           "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["TABS-main"]},
           "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": "Рынок ставок грузоперевозок"}},
           "TABS-main": {"type": "TABS", "id": "TABS-main", "children": [t[0] for t in TABS],
                         "parents": ["ROOT_ID"], "meta": {}}}
    for tab_id, tab_name, rows in TABS:
        row_ids = []
        for r, row in enumerate(rows):
            row_id = f"ROW-{tab_id}-{r}"
            row_ids.append(row_id)
            width = 12 // len(row)
            chart_ids = []
            for c, chart in enumerate(row):
                chart_id = f"CHART-{tab_id}-{r}-{c}"
                chart_ids.append(chart_id)
                height = 18 if chart["viz_type"] == "big_number_total" else 80 if chart["viz_type"] == "table" else 50
                pos[chart_id] = {"type": "CHART", "id": chart_id, "children": [],
                                 "parents": ["ROOT_ID", "TABS-main", tab_id, row_id],
                                 "meta": {"width": width, "height": height, "chartId": 0,
                                          "uuid": chart["uuid"], "sliceName": chart["title"]}}
            pos[row_id] = {"type": "ROW", "id": row_id, "children": chart_ids,
                           "parents": ["ROOT_ID", "TABS-main", tab_id], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        pos[tab_id] = {"type": "TAB", "id": tab_id, "children": row_ids, "parents": ["ROOT_ID", "TABS-main"],
                       "meta": {"text": tab_name, "defaultText": "Tab title", "placeholder": "Tab title"}}
    return pos


def native_filters() -> list[dict]:
    return [{
        "id": fid, "name": name, "filterType": "filter_select", "type": "NATIVE_FILTER", "description": "",
        "targets": [{"column": {"name": column}, "datasetUuid": uid(f"dataset:{dataset}")}],
        "controlValues": {"enableEmptyFilter": False, "defaultToFirstItem": False, "multiSelect": True,
                          "searchAllOptions": False, "inverseSelection": False},
        "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
        "cascadeParentIds": [], "scope": {"rootPath": [tab], "excluded": []},
    } for fid, name, dataset, column, tab in FILTERS]


def build_files() -> dict:
    client = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"], port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"], password=os.environ["CLICKHOUSE_PASSWORD"])
    db_uuid = uid("database:clickhouse")
    files = {
        "databases/ClickHouse_Analytics.yaml": {
            "database_name": DB_NAME,
            "sqlalchemy_uri": (f"clickhousedb://{os.environ['CLICKHOUSE_USER']}:{os.environ['CLICKHOUSE_PASSWORD']}"
                               f"@{os.environ['CLICKHOUSE_HOST']}:{os.environ.get('CLICKHOUSE_PORT', '8123')}/{CH_DB}"),
            "cache_timeout": None, "expose_in_sqllab": True, "allow_run_async": False, "allow_ctas": False,
            "allow_cvas": False, "allow_dml": False, "allow_csv_upload": False,
            "extra": {"allows_virtual_table_explore": True}, "uuid": db_uuid, "version": "1.0.0"},
    }
    for table in (MC, PR, MM):
        files[f"datasets/ClickHouse_Analytics/{table}.yaml"] = dataset_yaml(client, table, db_uuid)
    for _, _, rows in TABS:
        for row in rows:
            for chart in row:
                y = chart_yaml(chart)
                files[f"charts/{y['uuid']}.yaml"] = y
    files["dashboards/smart_logistics.yaml"] = {
        "dashboard_title": "Рынок ставок грузоперевозок", "description": None, "css": "",
        "slug": "smart-logistics", "certified_by": None, "certification_details": None, "published": True,
        "uuid": uid("dashboard:smart-logistics"), "position": position_json(),
        "metadata": {"native_filter_configuration": native_filters(), "color_scheme": "supersetColors",
                     "refresh_frequency": 0, "expanded_slices": {}, "label_colors": {}, "shared_label_colors": {},
                     "timed_refresh_immune_slices": [], "cross_filters_enabled": True, "default_filters": "{}",
                     "chart_configuration": {}, "global_chart_configuration": {
                         "scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "chartsInScope": []}},
        "version": "1.0.0",
    }
    return files


def bundle(files: dict, asset_type: str, prefixes: tuple[str, ...]) -> dict[str, str]:
    """Import contents for one asset level: metadata.yaml + the files that level needs."""
    contents = {name: yaml.safe_dump(body, allow_unicode=True, sort_keys=False)
                for name, body in files.items() if name.startswith(prefixes)}
    contents["metadata.yaml"] = yaml.safe_dump(
        {"version": "1.0.0", "type": asset_type, "timestamp": datetime.now(timezone.utc).isoformat()})
    return contents


def import_all(files: dict) -> None:
    from flask import g
    from superset.app import create_app

    app = create_app()
    with app.app_context(), app.test_request_context():
        # Superset modules read app.config at import time: import them inside the app context
        from superset import security_manager
        from superset.commands.chart.importers.v1 import ImportChartsCommand
        from superset.commands.dashboard.importers.v1 import ImportDashboardsCommand
        from superset.commands.database.importers.v1 import ImportDatabasesCommand
        from superset.commands.dataset.importers.v1 import ImportDatasetsCommand

        g.user = security_manager.find_user(username=os.environ["SUPERSET_ADMIN_USER"])
        levels = [
            (ImportDatabasesCommand, "Database", ("databases/",)),
            (ImportDatasetsCommand, "SqlaTable", ("databases/", "datasets/")),
            (ImportChartsCommand, "Slice", ("databases/", "datasets/", "charts/")),
            (ImportDashboardsCommand, "Dashboard", ("databases/", "datasets/", "charts/", "dashboards/")),
        ]
        for command, asset_type, prefixes in levels:
            command(bundle(files, asset_type, prefixes), overwrite=True).run()
            print(f"imported {asset_type} level ({sum(n.startswith(prefixes[-1]) for n in files)} objects)")


if __name__ == "__main__":
    import_all(build_files())
