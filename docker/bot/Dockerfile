# syntax=docker/dockerfile:1
#
# Telegram bot (long polling, runs forever): `alacati-bot`.
# Repo layout is mirrored under /opt/alacati so bot settings.REPO_ROOT resolves to
# /opt/alacati; config/ and contracts/ are bind-mounted read-only by docker-compose.yml.
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
    UV_PROJECT_ENVIRONMENT=/opt/venvs/bot

WORKDIR /opt/alacati/apps/telegram_bot
COPY apps/telegram_bot/pyproject.toml apps/telegram_bot/uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY apps/telegram_bot/src ./src
# Editable install (uv default): settings.REPO_ROOT = /opt/alacati.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev \
 && /opt/venvs/bot/bin/python -c "import alacati_bot" \
 && test -x /opt/venvs/bot/bin/alacati-bot

# --- runtime: same base, no uv, non-root ---------------------------------------------
FROM ${PYTHON_IMAGE}
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ALACATI_CONFIG_DIR=/opt/alacati/config \
    PATH=/opt/venvs/bot/bin:$PATH

COPY --from=builder /opt/venvs/bot /opt/venvs/bot
COPY --from=builder /opt/alacati /opt/alacati
COPY docker/bot/healthcheck.py /opt/alacati/docker/bot/healthcheck.py

RUN useradd --uid 10001 --gid 0 --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin bot \
 && mkdir -p /opt/alacati/config /opt/alacati/contracts

USER 10001
WORKDIR /opt/alacati/apps/telegram_bot
CMD ["/opt/venvs/bot/bin/alacati-bot"]
