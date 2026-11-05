#!/usr/bin/env bash
# One-shot init for the Airflow stack (service airflow-init, runs as root):
#   1. fail fast on missing / placeholder secrets from .env
#   2. make the shared log volume writable for the airflow user (uid 50000, gid 0)
#   3. run airflow-init-db.sh (db migrate + admin user) through the official entrypoint.
#      We do not use the entrypoint's _AIRFLOW_DB_MIGRATE switch: it runs
#      `airflow db migrate || true`, which would hide a failed migration.
set -euo pipefail

fail() { echo "airflow-init: ERROR: $*" >&2; exit 1; }
warn() { echo "airflow-init: WARNING: $*" >&2; }

# --- secrets -----------------------------------------------------------------------
fernet="${AIRFLOW__CORE__FERNET_KEY:-}"
[[ -n "$fernet" ]] || fail "AIRFLOW_FERNET_KEY is empty in .env"
# A Fernet key is 32 bytes, url-safe base64 encoded (stdlib check: works as root).
python3 - "$fernet" <<'PY' || fail "AIRFLOW_FERNET_KEY is not a valid Fernet key (see docs/laptop-server.md)"
import base64, binascii, sys
try:
    ok = len(base64.urlsafe_b64decode(sys.argv[1].encode())) == 32
except (binascii.Error, ValueError):
    ok = False
sys.exit(0 if ok else 1)
PY
for var in AIRFLOW__API_AUTH__JWT_SECRET AIRFLOW__API__SECRET_KEY AIRFLOW_ADMIN_USER AIRFLOW_ADMIN_PASSWORD; do
  value="${!var:-}"
  [[ -n "$value" ]] || fail "$var is empty (check the AIRFLOW_* entries in .env)"
  [[ "$value" != change-me* && "$value" != generate-* ]] || fail "$var still has its .env.example placeholder"
done

if [[ "${AIRFLOW_POSTGRES_PASSWORD:-}" == change-me* ]]; then
  warn "AIRFLOW_POSTGRES_PASSWORD still has its placeholder (Postgres is not exposed, but change it)"
fi

# --- mounts (GCP credentials are not mounted here: init never talks to BigQuery) --------
for d in /opt/alacati/config /opt/alacati/contracts; do
  [[ -n "$(ls -A "$d" 2>/dev/null)" ]] || fail "$d is empty: config/ and contracts/ must be mounted"
done

# --- shared volumes ------------------------------------------------------------------
mkdir -p /opt/airflow/logs
chown -R "50000:0" /opt/airflow/logs

# --- db migrate + admin user ----------------------------------------------------------
# `/entrypoint bash <script>` puts the airflow user's site-packages on PYTHONPATH for root.
exec /entrypoint bash /opt/alacati/docker/airflow-init-db.sh
