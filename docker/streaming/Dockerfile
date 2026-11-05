# syntax=docker/dockerfile:1
#
# Streaming consumers (one image, two commands):
#   alacati-bq-consumer   weather.raw -> BigQuery raw tables   (group bq-loader)
#   alacati-alert-engine  weather.raw -> lifecycle -> weather.alerts (group alert-engine)
#
# streaming/ depends on ../ingestion through a uv path dependency (contract validation and
# the BigQuery loader are reused, not copied), so both are laid out like the repo under
# /opt/alacati and installed editable: ingestion settings.REPO_ROOT = /opt/alacati.
# config/ and contracts/ are NOT baked in: docker-compose.yml bind-mounts them read-only.
#
# Build context: repo root.

ARG PYTHON_IMAGE=python:3.12.16-slim-bookworm
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.13.7

FROM ${UV_IMAGE} AS uv

# --- builder: resolve the locked environment with uv --------------------------------
FROM ${PYTHON_IMAGE} AS builder
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON=/usr/local/bin/python3.12 \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venvs/streaming

WORKDIR /opt/alacati
# Dependencies first (cached layer). The path dependency needs ingestion's metadata.
COPY ingestion/pyproject.toml ingestion/
COPY streaming/pyproject.toml streaming/uv.lock streaming/
RUN --mount=type=cache,target=/root/.cache/uv \
    cd streaming && uv sync --frozen --no-dev --no-install-project --no-install-package alacati-ingestion
# Then the code (editable installs).
COPY ingestion/src ingestion/src
COPY streaming/src streaming/src
RUN --mount=type=cache,target=/root/.cache/uv \
    cd streaming && uv sync --frozen --no-dev \
 && /opt/venvs/streaming/bin/python -c "import alacati_streaming.bq_consumer, alacati_streaming.alert_engine, confluent_kafka" \
 && test -x /opt/venvs/streaming/bin/alacati-bq-consumer \
 && test -x /opt/venvs/streaming/bin/alacati-alert-engine

# --- runtime: same base, no uv, non-root ---------------------------------------------
FROM ${PYTHON_IMAGE}
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ALACATI_CONFIG_DIR=/opt/alacati/config \
    HEARTBEAT_PATH=/tmp/alacati-heartbeat \
    PATH=/opt/venvs/streaming/bin:$PATH

COPY --from=builder /opt/venvs/streaming /opt/venvs/streaming
COPY --from=builder /opt/alacati /opt/alacati

RUN useradd --uid 10001 --gid 0 --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin streaming \
 && mkdir -p /opt/alacati/config /opt/alacati/contracts

USER 10001
WORKDIR /opt/alacati/streaming
# Healthy = the poll loop touched the heartbeat file within the last 2 minutes.
HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
  CMD ["python", "-c", "import os,sys,time; p=os.environ['HEARTBEAT_PATH']; sys.exit(0 if time.time()-os.path.getmtime(p) < 120 else 1)"]
CMD ["/opt/venvs/streaming/bin/alacati-bq-consumer"]
