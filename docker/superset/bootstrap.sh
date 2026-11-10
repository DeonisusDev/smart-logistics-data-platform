#!/usr/bin/env bash
# One-shot Superset initialisation (idempotent): metadata DB, admin, roles, data sources, dashboard.
set -euo pipefail
superset db upgrade
superset fab create-admin --username "${SUPERSET_ADMIN_USER}" --password "${SUPERSET_ADMIN_PASSWORD}" \
    --firstname Admin --lastname User --email admin@example.com   # prints "already exists" and exits 0 on reruns
superset init
# PostgreSQL DWH for ad-hoc SQL Lab queries over core/mart
superset set-database-uri --database_name "DWH PostgreSQL" --uri "${SUPERSET_DWH_URI}"
# ClickHouse database, datasets, charts and dashboard as code (overwrites by UUID on every run)
python /app/bootstrap/build_dashboard.py
echo "Superset bootstrap finished"
