"""Environment and config for the streaming consumers.

Weather thresholds and lifecycle parameters come from config/alerts.yaml. The values
below are operational knobs (timeouts, polling), not weather parameters; each can be
overridden by an environment variable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from alacati_ingestion.settings import CONFIG_DIR as REPO_CONFIG_DIR
from alacati_ingestion.settings import CONTRACTS_DIR

RAW_TOPIC = "weather.raw"
ALERTS_TOPIC = "weather.alerts"
BQ_LOADER_GROUP = "bq-loader"
ALERT_ENGINE_GROUP = "alert-engine"

ALERT_SCHEMA_PATH = CONTRACTS_DIR / "kafka" / "weather_alert.v1.schema.json"


def config_dir() -> Path:
    """ALACATI_CONFIG_DIR if set (Docker), else the repo's config/ folder."""
    raw = os.environ.get("ALACATI_CONFIG_DIR")
    return Path(raw) if raw else REPO_CONFIG_DIR


def load_yaml_config(name: str) -> dict[str, Any]:
    """Read config/<name>.yaml (not cached: an edited threshold applies on the next run)."""
    with (config_dir() / f"{name}.yaml").open(encoding="utf-8") as fh:
        data: dict[str, Any] = yaml.safe_load(fh)
    return data


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


def bootstrap_servers() -> str:
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not servers:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is not set")
    return servers


@dataclass(frozen=True)
class ConsumerSettings:
    bootstrap_servers: str
    # A run counts as closed when all run_expected_messages arrived, or this long after
    # its last message (a partial run, or a producer that died half-way).
    run_flush_timeout_s: float
    # Back-off between retries of a failed BigQuery side effect.
    retry_backoff_s: float
    retry_backoff_max_s: float
    heartbeat_path: Path

    @classmethod
    def from_env(cls) -> ConsumerSettings:
        return cls(
            bootstrap_servers=bootstrap_servers(),
            run_flush_timeout_s=_float_env("RUN_FLUSH_TIMEOUT_S", 120.0),
            retry_backoff_s=_float_env("RETRY_BACKOFF_S", 15.0),
            retry_backoff_max_s=_float_env("RETRY_BACKOFF_MAX_S", 300.0),
            heartbeat_path=Path(os.environ.get("HEARTBEAT_PATH", "/tmp/alacati-heartbeat")),  # noqa: S108
        )


@dataclass(frozen=True)
class EngineTimings:
    # How often the engine asks BigQuery whether dbt has built a closed run.
    mart_poll_interval_s: float
    # Give up on a run if dbt has not built it (or a newer run) within this time; the
    # next run is evaluated anyway. Covers the whole weather_hourly DAG budget (55 min).
    mart_wait_timeout_s: float

    @classmethod
    def from_env(cls) -> EngineTimings:
        return cls(
            mart_poll_interval_s=_float_env("MART_POLL_INTERVAL_S", 30.0),
            mart_wait_timeout_s=_float_env("MART_WAIT_TIMEOUT_S", 3600.0),
        )
