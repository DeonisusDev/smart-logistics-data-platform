# syntax=docker/dockerfile:1
FROM apache/superset:4.1.2
USER root
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
# DB drivers: PostgreSQL (metadata + DWH in SQL Lab) and ClickHouse (dashboard datasets)
RUN pip install psycopg2-binary==2.9.10 clickhouse-connect==0.8.18
COPY docker/superset/superset_config.py /app/pythonpath/superset_config.py
COPY docker/superset/bootstrap.sh docker/superset/build_dashboard.py /app/bootstrap/
RUN chmod +x /app/bootstrap/bootstrap.sh
USER superset
