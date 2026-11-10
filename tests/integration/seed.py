"""Small hand-crafted source dataset with every edge case the pipeline must handle.

Expected values in the tests are computed by hand from the rows below, not by re-running the SQL.
Hand-crafted deals use cities 1001-1003; the random ML sample uses 1004-1006 so it never touches
the mart cells asserted in test_mart.py.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np

AS_OF = date(2026, 10, 26)

CITIES = [  # (id, gc_id, name, full_name_local, lat, lon)
    (1, 1001, "Москва", "Москва, г. Москва", 55.7558, 37.6173),
    (2, 1002, "Казань", "Казань, Республика Татарстан", 55.7961, 49.1064),
    (3, 1003, "Екатеринбург", "Екатеринбург, Свердловская обл.", 56.8389, 60.6057),
    (4, 1004, "Новосибирск", "Новосибирск, Новосибирская обл.", 55.0084, 82.9357),
    (5, 1005, "Самара", "Самара, Самарская обл.", 53.1959, 50.1002),
    (6, 1006, "Краснодар", "Краснодар, Краснодарский край", 45.0355, 38.9753),
    # duplicated gc_id: same and different spelling - must not multiply deals
    (7, 1001, "Москва", "Москва, г. Москва", 55.7558, 37.6173),
    (8, 1002, "Казань", "Казань г, Татарстан", 55.7961, 49.1064),
]

BODY_TYPES = [  # (id, name, id_from_1C, std_id)
    (1, "бортовая", 1, 15), (2, "изотермический", 2, 2), (3, "контейнер", 3, 3), (4, "рефрижератор", 4, 4),
    (15, "борт", 15, 15), (46, "рефрижератор мультирежимный", 46, 4), (50, "самосвал", 50, 50),
    (56, "тентованный", 56, 56), (57, "тент бок", 57, 56), (61, "тент полная растентовка", 61, 56),
]
USD_RATE, EUR_RATE = 90.0, 100.0
RATE_DAYS = (date(2025, 1, 1), AS_OF)


def deal(pid, ld, lc=1001, uc=1002, *, price=100_000.0, distance=1000.0, weight=20.0, volume=80.0,
         body_type=56, currency="643", price_type=3, org=None, complex_=False, outlier=False,
         rate_date="auto", weight_category="auto", volume_category="auto", dates=None, end=None):
    """Return (prices row, price_body_types ids or None for default, price_dates list)."""
    ld = date.fromisoformat(ld) if isinstance(ld, str) else ld
    created = datetime.combine(ld - timedelta(days=2), datetime.min.time())
    if rate_date == "auto":
        rate_date = created.date()
    if weight_category == "auto":
        w = round(weight, 1)
        weight_category = ("G20T" if w >= 30 else "FTL" if w >= 10 else "10T" if w >= 5 else "5T" if w >= 2.5
                           else "L5T" if w >= 0.5 else "OTH")
    if volume_category == "auto":
        volume_category = "FTL" if 0 < volume <= 120 else "OTH"
    row = dict(id=pid, created_at=created, organization_id=org if org is not None else 500 + pid,
               loading_date=ld, loading_city_gcid=lc, unloading_city_gcid=uc, distance=distance,
               total_weight=weight, total_volume=volume, body_type=body_type, price=price, currency_code=currency,
               complex=complex_, price_type=price_type, is_fact=price_type in (1, 2, 3, 4, 5, 9),
               nearest_rate_date=rate_date, end_loading_date=end or ld, outlier_mark=outlier,
               weight_category=weight_category, volume_category=volume_category)
    return row, (dates or [ld])


# ----------------------------------------------------------------- hand-crafted deals
HAND = [
    # Cell A: 2026-09, Москва -> Казань, Тент, FTL. Valid: 100k/110k/120k, carriers = types 3 and 9 only.
    deal(1, "2026-09-03", price=100_000, price_type=3, org=701),
    deal(2, "2026-09-10", price=110_000, price_type=9, org=702),
    deal(3, "2026-09-17", price=120_000, price_type=1, org=703),
    deal(4, "2026-09-18", price=400_000, outlier=True),               # outlier: excluded
    deal(5, "2026-09-19", price=50_000, complex_=True),               # complex route: excluded
    deal(6, "2026-09-20", price=60_000, price_type=6),                # auction bid: not a fact
    deal(7, "2026-09-21", price=105_000, price_type=3, org=701,       # Тент twice via child id 57 -> once
         body_type=56),
    # Cell A history: Aug (single deal -> cv NULL), Jun (July missing -> Aug MoM is NULL), 2025-09 for YoY
    deal(10, "2026-08-12", price=100_000),
    deal(11, "2026-06-12", price=90_000),
    deal(12, "2025-09-12", price=88_000),

    # Route B Москва -> Екатеринбург: long-term request in three months, one future month and a planned month
    deal(20, "2026-08-15", 1001, 1003, price=150_000, end=date(2026, 12, 15),
         dates=[date(2026, 8, 15), date(2026, 9, 15), date(2026, 10, 15), date(2026, 11, 15), date(2026, 12, 15)]),

    # Route C Казань -> Екатеринбург: body groups
    deal(30, "2026-09-05", 1002, 1003, price=80_000, body_type=56),   # pbt 56 + 15 -> Тент and Борт
    deal(31, "2026-09-06", 1002, 1003, price=81_000, body_type=None),  # no pbt, NULL -> Не указан
    deal(32, "2026-09-07", 1002, 1003, price=82_000, body_type=4),    # no pbt rows -> fallback Рефрижератор
    deal(33, "2026-09-08", 1002, 1003, price=83_000, body_type=50),   # other std -> Прочие

    # Currency conversion and categories
    deal(40, "2026-09-09", 1003, 1002, price=1_000, currency="840"),  # USD 1000 * 90
    deal(41, "2026-09-09", 1003, 1002, price=1_000, currency="978"),  # EUR 1000 * 100
    deal(42, "2026-09-10", 1003, 1002, price=50_000, weight=9.96, volume=120,
         weight_category=None, volume_category=None),                  # gaps: 9.96 -> 10.0 -> FTL; 120 -> FTL
    deal(43, "2026-09-10", 1003, 1002, price=50_000, weight=0.44, volume=0,
         weight_category=None, volume_category=None),                  # OTH / OTH

    # DQ rejects
    deal(50, "2026-09-11", price=0),
    deal(51, "2026-09-11", price=500),                                 # RUB <= 1000
    deal(52, "2026-09-11", distance=0),
    deal(53, "2026-09-11", weight=-1),
    deal(54, "2026-09-11", volume=-5),
    deal(55, "2026-09-11", 9999, 1002),                                # unknown loading city
    deal(56, "2026-09-11", 1001, 9998),                                # unknown unloading city
    deal(57, "2026-09-11", price=1_000, currency="840", rate_date=None),            # no rate date
    deal(58, "2026-09-11", price=1_000, currency="978", rate_date=date(2020, 1, 1)),  # no exact rate
    deal(59, "2026-09-11", price=1_000, currency="398"),               # unsupported currency
    deal(60, "2026-09-11", price=0, distance=0),                       # two failed rules
    deal(61, "2026-09-11", 1003, 1002, price=500, currency="840"),     # 500 USD is fine (range is RUB only)
]
# Reliability: HIGH cell (>= 30 trips, >= 5 carriers, fresh) and MEDIUM cell (old month)
HAND += [deal(100 + i, AS_OF - timedelta(days=i % 20), 1003, 1001, price=70_000 + 100 * i, org=800 + i % 5)
         for i in range(30)]
HAND += [deal(200 + i, date(2026, 3, 2) + timedelta(days=i), 1003, 1001, price=60_000 + 100 * i, org=800 + i % 5)
         for i in range(12)]

EXTRA_BODY_TYPES = {7: [56, 57], 30: [56, 15]}   # price_id -> price_body_types ids
NO_BODY_TYPE_ROWS = {31, 32}

EXPECTED_QUARANTINE = {
    50: "NON_POSITIVE_PRICE", 51: "RUB_PRICE_OUT_OF_RANGE", 52: "NON_POSITIVE_DISTANCE", 53: "NEGATIVE_WEIGHT",
    54: "NEGATIVE_VOLUME", 55: "UNKNOWN_LOADING_CITY", 56: "UNKNOWN_UNLOADING_CITY", 57: "MISSING_EXACT_RATE",
    58: "MISSING_EXACT_RATE", 59: "UNSUPPORTED_CURRENCY", 60: "NON_POSITIVE_PRICE",
}


def ml_sample(n: int = 700, n_future: int = 40, seed: int = 7):
    """Random deals around the train/test windows and planned future requests (cities 1004-1006)."""
    rng = np.random.default_rng(seed)
    coords = {1004: (55.0, 82.9), 1005: (53.2, 50.1), 1006: (45.0, 39.0)}
    rows, pid = [], 10_000
    for i in range(n + n_future):
        lc, uc = rng.choice(list(coords), 2, replace=False)
        future = i >= n
        ld = AS_OF + timedelta(days=int(rng.integers(1, 60))) if future else \
            AS_OF - timedelta(days=int(rng.integers(0, 95)))
        dist = float(np.hypot(*np.subtract(coords[lc], coords[uc])) * 90)
        body = int(rng.choice([56, 4, 15]))
        price = round((75 + (20 if body == 4 else 0)) * dist + 6000) * float(rng.lognormal(0, 0.1))
        pid += 1
        rows.append(deal(pid, ld, int(lc), int(uc), price=price, distance=dist, body_type=body,
                         weight=float(rng.uniform(10, 25)), price_type=int(rng.choice([1, 2, 3, 9]))))
    # future rows that must NOT be scored: bid and complex route; plus an outlier in test (scored, not trained)
    rows.append(deal(pid + 1, AS_OF + timedelta(days=5), 1004, 1005, price_type=6))
    rows.append(deal(pid + 2, AS_OF + timedelta(days=5), 1004, 1005, complex_=True))
    rows.append(deal(pid + 3, AS_OF - timedelta(days=3), 1004, 1005, price=900_000, outlier=True))
    return rows


def load_source(conn) -> None:
    """Insert the dataset into a database that already has sql/schema.sql applied."""
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO cities (id, gc_id, name, full_name_local, full_name_gc, latitude, longitude)"
                        " VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        [(i, g, n, f, f, la, lo) for i, g, n, f, la, lo in CITIES])
        cur.executemany('INSERT INTO body_types (id, name, "id_from_1C", std_id) VALUES (%s, %s, %s, %s)', BODY_TYPES)
        start, end = RATE_DAYS
        cur.execute("""
            INSERT INTO exchange_rates (num_code, value, setting_date)
            SELECT c.code, c.v, d::date FROM generate_series(%s::date, %s::date, interval '1 day') d
            CROSS JOIN (VALUES ('643', 1.0::float8), ('840', %s::float8), ('978', %s::float8)) c(code, v)
        """, (start, end, USD_RATE, EUR_RATE))
        all_deals = HAND + ml_sample()
        columns = list(all_deals[0][0])
        with cur.copy(f"COPY prices ({', '.join(columns)}) FROM STDIN") as copy:
            for row, _ in all_deals:
                copy.write_row([row[c] for c in columns])
        pbt, pdates = [], []
        for row, dates in all_deals:
            pid = row["id"]
            if pid in EXTRA_BODY_TYPES:
                pbt += [(pid, b) for b in EXTRA_BODY_TYPES[pid]]
            elif row["body_type"] is not None and pid not in NO_BODY_TYPE_ROWS:
                pbt.append((pid, row["body_type"]))
            pdates += [(pid, d) for d in dates]
        cur.executemany("INSERT INTO price_body_types (price_id, body_type_id) VALUES (%s, %s)", pbt)
        with cur.copy("COPY price_dates (price_id, price_date) FROM STDIN") as copy:
            for r in pdates:
                copy.write_row(r)
