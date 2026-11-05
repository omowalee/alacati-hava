# syntax=docker/dockerfile:1
#
# Custom Airflow image for alacati-hava.
#
# Airflow's own Python environment is left untouched. The three components run from
# isolated uv venvs built from the repo's lock files:
#   /opt/venvs/ingestion  -> alacati-ingest
#   /opt/venvs/dbt        -> python /opt/alacati/dbt/run_dbt.py ...
#   /opt/venvs/bot        -> alacati-bot-summary
#
# Code is laid out like the repo under /opt/alacati so REPO_ROOT-relative paths
# (ingestion settings.py, dbt run_dbt.py, bot settings.py) resolve without code changes.
# config/ and contracts/ are NOT baked in: docker-compose.yml bind-mounts them read-only
# at /opt/alacati/config and /opt/alacati/contracts.
#
# Build context: repo root (see docker-compose.yml and .dockerignore).

ARG AIRFLOW_IMAGE=apache/airflow:3.3.2-python3.12
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.13.7

FROM ${UV_IMAGE} AS uv

# ---------------------------------------------------------------------------------------
# Builder: same base as the runtime image, so the venvs point at the same interpreter.
# ---------------------------------------------------------------------------------------
FROM ${AIRFLOW_IMAGE} AS builder
USER root
COPY --from=uv /uv /usr/local/bin/uv

# Use the image's python3.12, never download one; copy files out of the cache mount.
ENV UV_PYTHON=3.12 \
    UV_PYTHON_PREFERENCE=only-system \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /opt/alacati

# --- ingestion: dependencies first (cached layer), then the project itself (editable,
# so settings.REPO_ROOT = /opt/alacati) ---
COPY ingestion/pyproject.toml ingestion/uv.lock ingestion/
RUN --mount=type=cache,target=/root/.cache/uv \
    cd ingestion && UV_PROJECT_ENVIRONMENT=/opt/venvs/ingestion \
    uv sync --frozen --no-dev --no-install-project
COPY ingestion/src ingestion/src
RUN --mount=type=cache,target=/root/.cache/uv \
    cd ingestion && UV_PROJECT_ENVIRONMENT=/opt/venvs/ingestion \
    uv sync --frozen --no-dev

# --- dbt (package = false: only dependencies are installed) ---
COPY dbt/pyproject.toml dbt/uv.lock dbt/
RUN --mount=type=cache,target=/root/.cache/uv \
    cd dbt && UV_PROJECT_ENVIRONMENT=/opt/venvs/dbt \
    uv sync --frozen --no-dev
COPY dbt/ dbt/

# --- telegram bot (only alacati-bot-summary is used here; the long-polling bot has its
# own image, docker/bot/Dockerfile) ---
COPY apps/telegram_bot/pyproject.toml apps/telegram_bot/uv.lock apps/telegram_bot/
RUN --mount=type=cache,target=/root/.cache/uv \
    cd apps/telegram_bot && UV_PROJECT_ENVIRONMENT=/opt/venvs/bot \
    uv sync --frozen --no-dev --no-install-project
COPY apps/telegram_bot/src apps/telegram_bot/src
RUN --mount=type=cache,target=/root/.cache/uv \
    cd apps/telegram_bot && UV_PROJECT_ENVIRONMENT=/opt/venvs/bot \
    uv sync --frozen --no-dev

# Smoke test at build time: every entry point must import.
RUN /opt/venvs/ingestion/bin/python -c "import alacati_ingestion.cli" \
 && /opt/venvs/dbt/bin/python -c "import dbt.cli.main, dbt.adapters.bigquery" \
 && /opt/venvs/bot/bin/python -c "import alacati_bot" \
 && test -x /opt/venvs/ingestion/bin/alacati-ingest \
 && test -x /opt/venvs/bot/bin/alacati-bot-summary

# ---------------------------------------------------------------------------------------
# Runtime: official Airflow image + venvs + code. uv itself is not shipped.
# ---------------------------------------------------------------------------------------
FROM ${AIRFLOW_IMAGE}
USER root

COPY --from=builder /opt/venvs /opt/venvs
COPY --from=builder /opt/alacati /opt/alacati

# dbt profile: the repo's example profile (oauth = Application Default Credentials,
# everything else from env vars). dbt needs writable target/ and logs/ folders.
COPY dbt/profiles.example.yml /opt/alacati/dbt-profiles/profiles.yml
COPY docker/airflow/airflow-init.sh docker/airflow/airflow-init-db.sh /opt/alacati/docker/
# Strip CRLF in case the scripts were checked out on Windows without .gitattributes.
RUN sed -i 's/\r$//' /opt/alacati/docker/airflow-init.sh /opt/alacati/docker/airflow-init-db.sh \
 && chmod 0755 /opt/alacati/docker/airflow-init.sh /opt/alacati/docker/airflow-init-db.sh \
 && mkdir -p /opt/alacati/config /opt/alacati/contracts \
             /opt/alacati/dbt/target /opt/alacati/dbt/logs \
 && chown -R airflow:0 /opt/alacati/dbt/target /opt/alacati/dbt/logs /opt/alacati/dbt-profiles

ENV ALACATI_CONFIG_DIR=/opt/alacati/config \
    DBT_PROFILES_DIR=/opt/alacati/dbt-profiles \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    DBT_USE_COLORS=false

USER airflow
