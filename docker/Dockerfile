# syntax=docker/dockerfile:1
FROM python:3.11-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY common common
COPY etl etl
COPY ml ml
COPY sql sql
RUN useradd --create-home --uid 1000 app
USER app
CMD ["sh", "-c", "python -m etl.pipeline && python -m ml.train_score && python -m etl.clickhouse_sync"]

FROM runtime AS test
USER root
COPY requirements-dev.txt pytest.ini ./
RUN pip install -r requirements-dev.txt
COPY tests tests
USER app
CMD ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider"]
