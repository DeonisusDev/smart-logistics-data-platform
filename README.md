# Smart Logistics Data & ML Platform — решение тестового задания

Сквозной прототип **ETL → DWH → ML** для аналитики и прогнозирования тарифов грузоперевозок
на PostgreSQL 17 + Python — решение тестового задания на позицию Middle Data Engineer. Исходная схема
`sql/schema.sql` и генератор данных `sql/synthetic.sql` входят в материалы задания и используются без изменений.

## Быстрый старт

```bash
docker-compose up -d         # или: docker compose up -d   (нужно ~6 ГБ свободной RAM)
```

| Что | Где | Доступ |
| :--- | :--- | :--- |
| PostgreSQL 17 (источник + DWH) | `localhost:5432/deals_bigdata` | `postgres` / `postgres` |
| ClickHouse (аналитический слой) | http://localhost:8123 | `analytics` / `clickhouse` |
| Airflow (DAG `smart_logistics_rates`) | http://localhost:8080 | `admin` / `admin` |
| Superset (дашборд «Рынок ставок грузоперевозок») | http://localhost:8088/superset/dashboard/smart-logistics/ | `admin` / `admin` |

Что произойдёт (первый запуск ~8–10 минут, большая часть — генерация 300 000 сделок):

1. `postgres` создаёт исходную схему `sql/schema.sql`, детерминированные данные `sql/synthetic.sql` и служебные
   БД для Airflow и Superset. Healthcheck пропускает дальше только финальный сервер (проверка по TCP) после
   маркера готовности `sql/ready.sql`.
2. `pipeline` — обязательная часть одним прогоном: миграции → ETL (источник → `core` → `mart`) → обучение
   и batch scoring → синхронизация в ClickHouse со сверкой витрин. Завершается с кодом `0`
   (`docker-compose logs pipeline`).
3. Бонусы: `airflow-*` поднимают тот же пайплайн как DAG по расписанию (первый запуск стартует сам);
   `superset-init` после успешного `pipeline` загружает дашборд как код, `superset` его показывает.

Обязательная часть не зависит от бонусов: `docker-compose up pipeline` поднимет только PostgreSQL, ClickHouse
и прогон пайплайна. Повторный `up` безопасен — все шаги идемпотентны. Начать с нуля — `docker-compose down -v`.

> Старый `docker-compose` 1.29 поднимает стек с нуля без проблем, но на Docker Engine 25+ не умеет
> **пересоздавать** изменённые контейнеры (известная несовместимость v1). После правок кода используйте
> `docker compose` (v2) — например, `docker compose up -d --build`.
Параметры — в [`.env.example`](.env.example) (скопируйте в `.env`, чтобы переопределить; пароли выше — локальные
значения по умолчанию).

## Архитектура

```mermaid
flowchart LR
    subgraph RAW["public.* — источник (raw)"]
      P[prices] --- PBT[price_body_types] --- PD[price_dates]
      C[cities] --- BT[body_types] --- ER[exchange_rates]
    end
    subgraph CORE["core — очищенный слой"]
      DIM[dim_city<br/>dim_body_type]
      F[prices<br/>1 строка = 1 сделка, RUB]
      BR[price_body_groups<br/>bridge сделка × группа]
      CD[price_dates]
      Q[dq_quarantine_prices]
    end
    subgraph MART["mart — витрины"]
      MC[market_corridor]
      MP[ml_price_predictions]
      MM[ml_model_metrics]
    end
    RAW -- "etl/pipeline.py<br/>DQ, валюты, категории" --> CORE
    CORE -- "sql/mart/*.sql" --> MC
    CORE -- "ml/train_score.py" --> MP & MM
    OPS[(ops.schema_migrations<br/>ops.etl_runs)]
    subgraph CH["ClickHouse analytics — бонус"]
      CHC[core_* снапшоты]
      CHM[market_corridor<br/>MergeTree, считается в CH]
      CHP[ml_price_predictions<br/>ml_model_metrics]
    end
    CORE -- "etl/clickhouse_sync.py<br/>postgresql() + EXCHANGE" --> CHC --> CHM
    MART --> CHP
    CHM -. "сверка построчно" .- MC
    CH --> SS[Superset<br/>дашборд]
    AF[Airflow DAG] -. оркестрирует .-> RAW
```

| Слой | Таблица | Зерно / назначение |
| :--- | :--- | :--- |
| raw | `public.*` | Операционная схема из `sql/schema.sql`, пайплайн её только читает |
| core | `dim_city` | `gc_id` (дедупликация: минимальный `cities.id` на `gc_id`) |
| core | `dim_body_type` | Код кузова → `std_id` → бизнес-группа. Маппинг групп задан только здесь |
| core | `prices` | Валидная сделка: цена в RUB, категории, **основная** группа кузова (для ML) |
| core | `price_body_groups` | Сделка × стандартная группа кузова: «тент + борт» → по одной строке в каждой группе, внутри группы без дублей |
| core | `price_dates` | Помесячная развёртка долгосрочных заявок |
| core | `dq_quarantine_prices` | Отбракованные строки: основная причина, все причины, снимок исходной строки (`jsonb`) |
| mart | `market_corridor` | Месяц × откуда × куда × группа кузова × весовая категория |
| mart | `ml_price_predictions` | Сделка × модель: факт и прогноз, `split = test / future` |
| mart | `ml_model_metrics` | Метрики основной модели и бейзлайнов на каждом запуске |
| ops | `schema_migrations`, `etl_runs` | Версии DDL и журнал запусков (параметры, метрики, статус, ошибка) |
| ClickHouse | `analytics.*` | Снапшоты core/ML и витрина `market_corridor`, посчитанная в ClickHouse (бонус) |

### Структура репозитория

```
common/            конфиг из env, подключение и миграции, журнал запусков, структурированные логи
etl/pipeline.py    Этап 1–2: загрузка core + пересчёт витрины (CLI с окном дат)
etl/dq_gate.py     DQ-гейт после загрузки (доля карантина, консистентность) — блокирует ML и публикацию
etl/clickhouse_sync.py  бонус: снапшот в ClickHouse, витрина на MergeTree, сверка с PostgreSQL
dags/              бонус: Airflow DAG smart_logistics_rates
ml/features.py     Этап 3: признаки и хронологическое разбиение (чистые функции)
ml/models.py       CatBoost + бейзлайны, все преобразования внутри sklearn Pipeline
ml/train_score.py  обучение, метрики, batch scoring в БД
sql/schema.sql, sql/synthetic.sql   исходные материалы задания (без изменений)
sql/migrations/    версионированный DDL слоёв ops / core / mart
sql/etl/           загрузка core (все DQ-правила в одном месте)
sql/mart/          витрина коридора ставок
sql/clickhouse/    DDL, загрузка и витрина для ClickHouse
docker/Dockerfile  multi-stage: runtime и test
docker/airflow/, docker/superset/, docker/clickhouse/, docker/postgres/   образы и конфиги бонусных сервисов
tests/unit/        признаки, разбиение, модели, SQL-файлы — без БД
tests/integration/ ETL, витрина, идемпотентность, скоринг, ClickHouse — на временной БД
tests/dags/        целостность DAG (выполняется в образе Airflow)
```

## Этап 1. ETL и Data Quality

Выполняется в `etl/pipeline.py`, сами трансформации — set-based SQL в `sql/etl/load_core.sql`
(`INSERT … SELECT`, `COPY`, без построчных вставок). Все DQ-правила вычисляются **один раз** во временной
staging-таблице. Из неё читают и `core`, и карантин, поэтому правила для двух таблиц не могут разойтись.

| Код причины | Правило | Строк в синтетике |
| :--- | :--- | ---: |
| `NON_POSITIVE_PRICE` | `price <= 0` | 164 |
| `RUB_PRICE_OUT_OF_RANGE` | `currency_code = 643 AND price <= 1000` | 0 |
| `NON_POSITIVE_DISTANCE` | `distance <= 0` | 0 |
| `NEGATIVE_WEIGHT` / `NEGATIVE_VOLUME` | `total_weight < 0` / `total_volume < 0` | 0 |
| `UNKNOWN_LOADING_CITY` / `UNKNOWN_UNLOADING_CITY` | города нет в `cities` | 0 |
| `UNSUPPORTED_CURRENCY` | валюта не 643/840/978 | 0 |
| `MISSING_EXACT_RATE` | для 840/978 нет курса ровно на `nearest_rate_date` (в т.ч. `NULL`) | 39 |

Итог на 300 000 сделок: **299 797** в `core.prices`, **203** в карантине, ни одна строка не потеряна.
Строка может нарушать несколько правил. Тогда `reason_code` — первое по приоритету правило, а `reason_codes` — все нарушенные.

Решения:

* **Валюта.** `price_rub = price × курс ЦБ` строго на `nearest_rate_date`, как требуют условия.
  Для 643 курс равен 1. Fallback по `loading_date` не используется. Дубли в `exchange_rates`, если они появятся, не размножат сделку.
* **Категории.** `weight_category` / `volume_category` из источника сохраняются, а **пропуски** заполняются по
  правилам задания (вес с округлением до 0.1 т; объём `FTL` при `0 < V ≤ 120`).
* **Кузов.** Группы строятся по `std_id`: Тент 56, Рефрижератор 4, Изотерм 2, Борт 15, иначе Прочие,
  `NULL` → Не указан. Если у сделки нет строк в `price_body_types`, группа берётся по `prices.body_type`.
* **`outlier_mark`** используется как входной флаг и не пересчитывается. Дополнительный пересчёт
  в стиле `MarkOutlierPrices` не реализован, так как он необязательный.
* **Маршруты «город A → город A»** не отбраковываются: это не production-DQ-правило задания.

> **Ограничение исходных данных.** Сделки последнего дня снимка (`2026-10-26`) ещё не прошли ночную разметку
> аномалий: у них `outlier_mark = false` по умолчанию, и часть выбросов этого дня попадает в витрину
> и в test-выборку ML. Это свойство источника. Оно задокументировано, но не «исправляется» в базовом решении.

### Идемпотентность и backfill

Окно задаётся по `loading_date` (включительно): переменными `START_DATE` / `END_DATE` или флагами CLI. Пустое окно — весь источник.
Вместе с плановыми погрузками после `AS_OF`, которые нужны для скоринга.

```bash
# перезагрузить сентябрь 2026 (postgres уже запущен)
docker-compose run --rm pipeline python -m etl.pipeline --start-date 2026-09-01 --end-date 2026-09-30
# только core, без пересчёта витрины
docker-compose run --rm pipeline python -m etl.pipeline --start-date 2026-09-01 --end-date 2026-09-30 --skip-mart
```

* Загрузка окна — атомарная перезапись партиции `DELETE + INSERT` **в одной транзакции** вместе с
  карантином и витриной. При ошибке ничего не меняется. Связанные строки bridge и дат удаляются через `ON DELETE CASCADE`.
* Удаление идёт и по окну, и по `price_id` из staging. Поэтому сделка, у которой в источнике сдвинулась `loading_date`, не задвоится.
* Параллельные запуски сериализуются `pg_advisory_xact_lock`. Миграции применяются один раз,
  а изменение уже применённой миграции останавливает запуск: нужно добавить новую.
* Проверено: повторный полный прогон и backfill окна дают идентичные количества и суммы во всех таблицах
  (покрыто тестом `test_rerun_and_backfill_are_idempotent`).

Журнал запусков: `SELECT run_id, job, status, metrics, error FROM ops.etl_runs ORDER BY run_id DESC;`

Пример структурированного лога (`LOG_FORMAT=json` переключает вывод в JSON lines):

```
INFO etl.pipeline event=core_loaded rows_read=300000 rows_loaded=299797 rows_quarantined=203
     quarantine_by_reason={"MISSING_EXACT_RATE": 39, "NON_POSITIVE_PRICE": 164} body_group_rows=335077 ...
INFO etl.pipeline event=mart_refreshed table="mart.market_corridor" rows=203644
     reliability={"HIGH": 10, "LOW_DATA": 202710, "MEDIUM": 924}
```

## Этап 2. Витрина «Карточка маршрута и коридор ставок»

`mart.market_corridor` полностью пересобирается в той же транзакции, что и загрузка (SQL — `sql/mart/market_corridor.sql`).
**Зерно:** месяц `price_dates.price_date` × город отправления × город назначения × группа кузова × весовая категория.

**Фильтры:** `is_fact AND price_type IN (1,2,3,4,5,9)`, `NOT complex`, `NOT outlier_mark`, `price_rub > 0`,
`distance > 0`, `price_date <= as_of`. Плановые месяцы в историю рынка не попадают. Долгосрочная заявка
попадает в каждый свой месяц, но внутри месяца учитывается один раз.

| Метрика | Определение |
| :--- | :--- |
| `p25_trip`, `median_trip`, `p75_trip`, `avg_trip` | `percentile_cont` / `avg` по `price_rub` |
| `p25_km`, `median_km`, `p75_km` | то же по `price_rub / distance` |
| `trips_count` | `count(DISTINCT price_id)` |
| `carriers_count` | `count(DISTINCT organization_id)` при `price_type IN (3, 9)` |
| `cv_rate` | `stddev_samp / avg`; `NULL` при одном наблюдении или нулевом среднем |
| `mom_pct`, `yoy_pct` | изменение P50 к **календарному** предыдущему месяцу / тому же месяцу год назад; нет ячейки → `NULL` |
| `freshness_days` | `as_of − max(price_date)` в ячейке |
| `reliability_level` | `HIGH`: trips ≥ 30, carriers ≥ 5, freshness ≤ 30 дней; `MEDIUM`: trips ≥ 10; иначе `LOW_DATA` |

Решения:

* Сделка с двумя совместимыми кузовами (тент + борт) входит в ячейку каждой группы через bridge, но никогда
  не учитывается дважды в одной группе. Дочерние коды (57 → 56) схлопываются.
* «Свежесть последнего месяца» понимается как дни от последней `price_date` в ячейке до `as_of`.
  Поэтому `HIGH` возможен только у текущего месяца: это осознанно строгое прочтение требования.
* `volume_category` не входит в зерно, как и требует задание, и не хранится: в ячейке смешаны разные
  объёмные категории, и одно значение вводило бы в заблуждение.
* Индексы: PK по зерну (сводная таблица по месяцу) и `(route, body, weight, month)` для детальной карточки маршрута.
* Корректность проверена независимым SQL-пересчётом напрямую из `public.*`: все 203 644 ячейки совпали.

Примеры запросов:

```sql
-- Детальная карточка маршрута: Москва -> Казань, тент, FTL
SELECT price_month, p25_trip, median_trip, p75_trip, median_km, trips_count, carriers_count,
       round(cv_rate::numeric, 3) AS cv, round(mom_pct::numeric, 1) AS mom, round(yoy_pct::numeric, 1) AS yoy,
       reliability_level
FROM mart.market_corridor
WHERE loading_city_gcid = 1001 AND unloading_city_gcid = 1003 AND body_group = 'Тент' AND weight_category = 'FTL'
ORDER BY price_month DESC;

-- Сводка сети за последний месяц: самые надёжные направления
SELECT loading_city, unloading_city, body_group, weight_category, median_trip, median_km, trips_count, reliability_level
FROM mart.market_corridor
WHERE price_month = '2026-10-01'
ORDER BY trips_count DESC LIMIT 20;
```

| price_month | p25 | p50 | p75 | p50, руб./км | trips | carriers | cv | MoM % | YoY % | reliability |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 2026-10-01 | 84 735 | 95 019 | 104 908 | 105.9 | 37 | 9 | 0.175 | 1.3 | 10.1 | HIGH |
| 2026-09-01 | 85 630 | 93 834 | 105 044 | 104.6 | 29 | 10 | 0.166 | 5.1 | 12.7 | MEDIUM |
| 2026-08-01 | 82 028 | 89 277 | 104 369 | 99.5 | 25 | 10 | 0.180 | 1.8 | 0.4 | MEDIUM |

## Этап 3. ML: признаки, обучение, batch scoring

**Таргет:** `price_rub` (RUB без НДС). Модель обучается на `log1p(price_rub)`: тарифы логнормальны, и такая
функция потерь штрафует относительную ошибку (то, что меряет MAPE). Метрики считаются в рублях.

**Выборка:** `is_fact`, `NOT complex`, `price_rub > 0`, `distance > 0` (гарантировано core), таргет очищен
по `outlier_mark`. Флаг в признаки не входит.

**Разбиение** строго по `loading_date` относительно `AS_OF`, а не системной даты:
`train = [2026-07-26, 2026-10-05)` — 24 226 сделок, `test = [2026-10-05, 2026-10-26]` — 12 808 сделок.

**Признаки** (`ml/features.py`, белый список; тест проверяет, что ни один запрещённый столбец не попадает в матрицу):

* география: регион и город отправления и назначения **раздельно** (разбор `full_name_local` по первой запятой),
  координаты из дедуплицированного справочника;
* груз и транспорт: `distance`, `log_distance`, `total_weight`, `total_volume`, основная группа кузова,
  весовая и объёмная категории, `price_type` (тип заявки известен в момент её создания);
* календарь `loading_date`: `year`, `month`, `day_of_week`, `day_of_year` и их `sin/cos` (T = 12 / 7 / 365.25).

**Защита от утечек.** Исключены `price`, `price_rub`, ставка с НДС, удельные тарифы, маржа и стоимость,
`exchange_rate`, всё из `price_group_stats` и `outlier_mark`. Энкодеры, импутеры и скейлеры живут внутри
`sklearn.Pipeline` и обучаются только на `train`. Сиды зафиксированы (`RANDOM_SEED=42`), результат воспроизводим:
это тоже проверяется тестом.

### Метрики на test (`AS_OF = 2026-10-26`)

| Модель | Роль | R² | MAPE | RMSE, руб. | MAE, руб. |
| :--- | :--- | ---: | ---: | ---: | ---: |
| `CatBoostRegressor` (нативные категории) | **основная** | **0.926** | **14.13%** | **51 866** | **26 500** |
| `Ridge` (One-Hot + Scaler) | бейзлайн | 0.929 | 13.88% | 51 045 | 26 012 |
| `HistGradientBoostingRegressor` | бейзлайн | 0.923 | 14.49% | 53 106 | 27 209 |

Метрики пишутся в лог и в `mart.ml_model_metrics` на каждом запуске.

Как читать результаты:

* Основная модель (CatBoost, как рекомендует R&D-записка) выбрана **заранее**, а не по test-метрикам.
  Выбор по test сам по себе был бы утечкой. Для честного выбора нужен отдельный validation-фолд внутри train.
* Ridge здесь не хуже бустинга, и это ожидаемо для этих данных. `sql/synthetic.sql` строит цену как
  `(k·distance + 6000) × сезонность × множители × логнормальный шум`, то есть в лог-пространстве почти линейно.
  На реальных данных с нелинейными взаимодействиями ожидается заметный разрыв в пользу бустинга.
* Нижняя граница MAPE задана шумом генератора (σ = 0.16 → ~12.8%), так что модели близки к пределу.
  Неразмеченные выбросы `2026-10-26` на метрику почти не влияют: MAPE за этот день 13.4%, за остальные дни test — 14.2%.

### Batch scoring → `mart.ml_price_predictions`

* `split = 'test'` — все фактические прямые сделки тестового периода, **включая** помеченные выбросы
  (`outlier_mark` хранится рядом). Так «Факт vs Прогноз» подсвечивает рейсы, закрытые заметно выше рынка;
  метрики при этом считаются по очищенной выборке. 13 078 строк, из них 270 выбросов.
* `split = 'future'` — плановые фактические прямые заявки с `loading_date > AS_OF` (4 891 строка).
  `actual_price_rub`, `abs_error_rub`, `ape_pct` = `NULL`, даже если в источнике есть `price`.
  Это гарантирует и CHECK-ограничение таблицы. Ставки торгов и составные маршруты не скорятся: модель на них не обучалась.
* Идемпотентность: в одной транзакции удаляется весь скорируемый диапазон модели, затем выполняется `COPY` нового.

```sql
-- Факт vs Прогноз: рейсы, закрытые более чем на 30% выше прогноза
SELECT price_id, loading_date, body_group, actual_price_rub, round(predicted_price_rub) AS predicted, round(ape_pct, 1) AS ape
FROM mart.ml_price_predictions
WHERE split = 'test' AND actual_price_rub > 1.3 * predicted_price_rub
ORDER BY ape_pct DESC LIMIT 20;

-- Метрики последних запусков
SELECT etl_run_id, model_name, is_primary, r2, mape_pct, rmse_rub, mae_rub FROM mart.ml_model_metrics ORDER BY 1 DESC, 3 DESC;
```

## Бонусы: ClickHouse, Airflow, Superset

Все три бонуса дают **те же результаты**, что и обязательная часть: они вызывают тот же код и тот же SQL,
а ClickHouse-витрина после каждой загрузки сверяется с PostgreSQL построчно.

### ClickHouse — аналитический слой на MergeTree

`etl/clickhouse_sync.py`, SQL — `sql/clickhouse/`:

1. **Снапшот core и ML-таблиц.** Таблицы забираются табличной функцией `postgresql()`, которую выполняет сам
   сервер ClickHouse, поэтому данные не проходят через Python. Доступ к PostgreSQL описан в именованной
   коллекции `pg_dwh` (`docker/clickhouse/config.d`): пароли берутся из окружения контейнера и не попадают
   ни в SQL, ни в `query_log`.
2. **Витрина `market_corridor` считается в ClickHouse** из реплицированных core-таблиц по тем же правилам.
   `percentile_cont` ↔ `quantileExactInclusive` (обе используют линейную интерполяцию), `count(DISTINCT)` ↔
   `uniqExact`, календарные MoM/YoY через `LEFT JOIN ... SETTINGS join_use_nulls = 1`. Таблица — `MergeTree`,
   `ORDER BY (маршрут, кузов, вес, месяц)` под сценарий карточки маршрута, партиции по году.
3. **Сверка (parity check).** Витрины PostgreSQL и ClickHouse сравниваются построчно: ключи и все 18 метрик,
   допуск по float 1e-9, `NULL` = `NULL`. Любое расхождение роняет задачу. Результат на полных данных:
   `clickhouse_rows=203644 postgres_rows=203644 matched_rows=203644 mismatched_rows=0`.

Идемпотентность: каждая таблица грузится в `<table>__stage` и подменяется атомарно через `EXCHANGE TABLES`.
Повтор безопасен, читатели (Superset) не видят полузагруженную таблицу, а строки, исчезнувшие из источника,
исчезают и здесь. `ReplacingMergeTree` сознательно не используется: он дедуплицирует асинхронно (нужен
`FINAL`) и не умеет удалять строки без tombstone-колонки. При таком объёме полная подмена занимает секунды.
Инкрементальный вариант для больших объёмов — подмена партиций (`REPLACE PARTITION`) по окну ETL.

```sql
-- ClickHouse: карточка маршрута
SELECT price_month, median_trip, p25_trip, p75_trip, trips_count, reliability_level
FROM analytics.market_corridor
WHERE route = 'Москва → Казань' AND body_group = 'Тент' AND weight_category = 'FTL'
ORDER BY price_month DESC;
```

### Airflow — DAG `smart_logistics_rates`

`etl_core_mart` → `dq_gate` → `ml_train_score` → `clickhouse_sync`, ежедневно в 03:00 МСК, `catchup=False`,
`max_active_runs=1`, ретраи с экспоненциальной задержкой и `execution_timeout`.

* **Окно.** По умолчанию `[logical_date − lookback_days (30), +∞)`: подхватываются поздно пришедшие сделки
  за последние дни и плановые погрузки для скоринга.
* **Backfill.** *Trigger DAG w/ config* `{"start_date": "2026-09-01", "end_date": "2026-09-30"}` или
  `airflow dags backfill`. Проверено: такой запуск перезагружает ровно сентябрь (13 767 строк источника).
* **DQ-гейт.** ETL печатает `run_id` последней строкой, он уходит в XCom, и `dq_gate` проверяет именно этот
  запуск: долю карантина (порог — параметр DAG), сделки без `price_dates`, сделки без группы кузова, дубли
  ячеек витрины. Гейт не ретраится: проваленную проверку качества нельзя «перезапустить до зелёного».
* **Изоляция зависимостей.** Задачи — `BashOperator`, который вызывает те же CLI, что и `pipeline`, но из
  отдельного virtualenv в образе (`/opt/pipeline/venv`). Ограничения версий Airflow не конфликтуют
  с пайплайном, а код запускается одинаково в compose и в Airflow.
* **Конкурентность.** ETL и ML сериализуются advisory-локами PostgreSQL, поэтому одновременный прогон DAG
  и контейнера `pipeline` безопасен. При первом `up` они и правда идут параллельно, и оба завершаются успешно.
* Метаданные Airflow хранятся в отдельной БД `airflow` на том же PostgreSQL, исполнитель — `LocalExecutor`.

### Superset — дашборд «Рынок ставок грузоперевозок»

Источник — ClickHouse. Дашборд описан **кодом** (`docker/superset/build_dashboard.py`): база, 3 датасета,
15 графиков, 3 вкладки и нативные фильтры. Колонки датасетов читаются из `system.columns`, UUID
детерминированные, а каждый уровень (база → датасеты → графики → дашборд) импортируется с `overwrite=True`.
Поэтому повторный bootstrap обновляет объекты, а не плодит копии. Стандартный `superset import-dashboards`
перезаписывает только сам дашборд, а не его графики.

* **Рыночный коридор:** KPI последнего месяца, коридор P25–P50–P75 в `руб./рейс` и `руб./км`, доказательная
  база (рейсы и перевозчики), доля рейсов по уровню надёжности, сводная таблица сети с MoM/YoY.
  Фильтры «Маршрут / Группа кузова / Весовая категория» превращают вкладку в детальную карточку маршрута.
* **Факт vs Прогноз:** MAPE/MAE на test, число оценённых будущих заявок, дневная динамика факта и прогноза
  (справа от `as_of` — только прогноз), рейсы, закрытые дороже прогноза более чем на 30%.
* **Качество модели:** метрики CatBoost и бейзлайнов последнего запуска.
* «Последний месяц» и «последний запуск» выражены через колонки данных (`toStartOfMonth(as_of)`,
  `is_latest_run`), а не через подзапросы. Подзапросы в ad-hoc SQL Superset запрещены по умолчанию, и этот
  запрет разумно не снимать.
* Для ad-hoc запросов к `core`/`mart` в SQL Lab зарегистрирована база «DWH PostgreSQL».

## Тесты

```bash
docker-compose run --rm tests          # всё в контейнере: временные БД в PostgreSQL и ClickHouse
docker-compose exec airflow-scheduler python -m pytest /opt/airflow/tests -q   # целостность DAG
# или локально (Python 3.11+):
pip install -r requirements-dev.txt
TEST_DB_DSN=postgresql://postgres:postgres@localhost:5432/postgres pytest
```

* `tests/unit` — разбор регионов, циклические признаки, границы time split, отсутствие утечек в матрице
  признаков, детерминизм моделей и обработка новых категорий, корректность параметров в SQL-файлах,
  решения DQ-гейта, валидация идентификаторов и полнота сверки для ClickHouse.
* `tests/integration` — создают отдельную временную БД, загружают `sql/schema.sql` и вручную составленный набор
  краевых случаев (`tests/integration/seed.py`): каждый DQ-код, дубли `gc_id`, валюты, заполнение категорий,
  кузова (bridge, дочерние коды, fallback, `NULL`), долгосрочные заявки, MoM при пропущенном месяце, YoY,
  уровни надёжности. Плюс идемпотентность повторного прогона и backfill, а также скоринг (будущие без факта,
  ставки торгов не скорятся, воспроизводимость). ClickHouse: сверка витрин, совпадение числа строк,
  вручную посчитанная ячейка, идемпотентность повторной синхронизации. Ожидаемые значения посчитаны вручную.
  Без доступных серверов интеграционные тесты пропускаются с понятным сообщением.
* `tests/dags` — DAG импортируется без ошибок, порядок задач, `catchup`, таймауты, шаблоны окна и XCom.

## Конфигурация

| Переменная | По умолчанию | Назначение |
| :--- | :--- | :--- |
| `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_PORT` | `deals_bigdata` / `postgres` / `postgres` / `5432` | БД в docker-compose |
| `DB_DSN` | собирается из `POSTGRES_*` | Подключение для ETL/ML (на хосте задаётся вручную) |
| `AS_OF` | `2026-10-26` | Дата снимка («now» задания) |
| `START_DATE` / `END_DATE` | пусто = весь источник | Окно ETL по `loading_date` |
| `RANDOM_SEED` | `42` | Сид всех моделей |
| `MODEL_NAME` | `catboost` | Модель для скоринга: `catboost` / `hist_gradient_boosting` / `ridge` |
| `LOG_FORMAT` | `text` | `text` (key=value) или `json` |
| `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_HTTP_PORT` | `clickhouse` / `8123` | Пользователь `analytics` в ClickHouse |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` / `AIRFLOW_PORT` | `admin` / `admin` / `8080` | Airflow UI |
| `AIRFLOW_SECRET_KEY`, `AIRFLOW_FERNET_KEY` | демо-значение / пусто | Секреты Airflow |
| `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` / `SUPERSET_PORT` | `admin` / `admin` / `8088` | Superset UI |
| `SUPERSET_SECRET_KEY` | демо-значение | Ключ шифрования Superset |

## Обоснование стека

* **PostgreSQL 17** — транзакционная атомарная перезапись окна, `percentile_cont`, `FILTER`, CHECK/FK-ограничения
  как последняя линия DQ. На сотнях тысяч строк отдельное OLAP-хранилище не нужно: полный ETL занимает около минуты.
* **Python + psycopg 3, ELT-подход** — тяжёлая работа выполняется set-based SQL внутри БД. Python оркеструет шаги,
  логирует количество строк по каждому шагу и ведёт журнал запусков. ML использует `COPY`.
* **CatBoost** — рекомендованная модель для табличных данных с категориями (нативные `cat_features`),
  **scikit-learn** — бейзлайны и единый `Pipeline` для препроцессинга без утечек.
* **Docker Compose** — один `docker-compose up`, multi-stage образ (runtime без тестовых зависимостей,
  отдельный target для тестов), процесс в контейнере работает не от root. Версии всех образов зафиксированы.
* **ClickHouse 24.8 LTS** — колоночный движок для BI-нагрузки: агрегаты по сотням тысяч ячеек без нагрузки
  на транзакционный PostgreSQL.
* **Airflow 2.10 (LocalExecutor)** — расписание, ретраи, backfill и наблюдаемость для того же CLI-пайплайна.
* **Superset 4.1** — open-source BI с нативными фильтрами и драйвером ClickHouse, дашборд хранится в коде.

## Что сознательно не сделано и что дальше

* Витрина PostgreSQL и снапшоты ClickHouse пересобираются целиком. Для production-объёмов нужно пересчитывать
  только затронутые месяцы (плюс соседние для MoM/YoY), а в ClickHouse подменять партиции.
* Для будущих заявок модель стоит дообучать на `train + test`, а гиперпараметры подбирать на validation-фолде.
* Опциональный пересчёт выбросов (IQR по `price`, P5–P95 по `price_km` в группах ≥ 4 строк) не реализован.
  Исходный `outlier_mark` используется как есть.
* Демо-стек использует пароли и секреты по умолчанию из `.env.example`. Для общего стенда их нужно задать
  в `.env` или в secret manager. Superset работает без Redis (SimpleCache): для одного узла этого достаточно.
