# syntax=docker/dockerfile:1
# Airflow with the pipeline installed into its own virtualenv (/opt/pipeline/venv):
# Airflow's pinned constraints (numpy/pandas/...) and the pipeline's requirements never conflict.
FROM apache/airflow:2.10.5-python3.11
USER root
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements.txt /opt/pipeline/requirements.txt
RUN python -m venv /opt/pipeline/venv \
 && /opt/pipeline/venv/bin/pip install -r /opt/pipeline/requirements.txt
COPY common /opt/pipeline/common
COPY etl /opt/pipeline/etl
COPY ml /opt/pipeline/ml
COPY sql /opt/pipeline/sql
COPY --chown=airflow:root dags /opt/airflow/dags
USER airflow
# DAG integrity tests: docker-compose exec airflow-scheduler python -m pytest /opt/airflow/tests -q
RUN pip install --no-cache-dir pytest==8.3.3
COPY --chown=airflow:root tests/dags /opt/airflow/tests
ENV DAGS_DIR=/opt/airflow/dags
