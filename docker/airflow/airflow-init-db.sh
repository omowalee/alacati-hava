#!/usr/bin/env bash
# Called by airflow-init.sh through the official entrypoint. Fails loudly on errors.
set -euo pipefail

airflow version
airflow db migrate

# Admin user for the UI (FAB auth manager). Idempotent: an existing user is kept as is;
# to change the password later use `airflow users reset-password` (docs/laptop-server.md).
user="${AIRFLOW_ADMIN_USER:?}"
if ! airflow users create \
      --username "$user" \
      --password "${AIRFLOW_ADMIN_PASSWORD:?}" \
      --firstname Alacati --lastname Admin \
      --email "admin@localhost" \
      --role Admin; then
  # Non-zero exit: accept it only if the user really exists.
  airflow users list --output json \
    | python3 -c 'import json, sys; sys.exit(0 if any(u.get("username") == sys.argv[1] for u in json.load(sys.stdin)) else 1)' "$user" \
    || { echo "airflow-init: ERROR: could not create admin user '$user'" >&2; exit 1; }
  echo "airflow-init: admin user '$user' already exists"
fi
echo "airflow-init: done"
