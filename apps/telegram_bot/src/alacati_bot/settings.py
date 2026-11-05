"""Configuration: repo YAML files and environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_CONFIG_DIR = REPO_ROOT / "config"


def load_env() -> None:
    """Load the repo-root .env if present (never overrides real env vars)."""
    load_dotenv(REPO_ROOT / ".env")


def config_dir() -> Path:
    """Config directory: ALACATI_CONFIG_DIR or <repo>/config."""
    env = os.environ.get("ALACATI_CONFIG_DIR")
    return Path(env) if env else DEFAULT_CONFIG_DIR


def contracts_dir() -> Path:
    """Contracts directory: ALACATI_CONTRACTS_DIR, else the `contracts` sibling of config/."""
    env = os.environ.get("ALACATI_CONTRACTS_DIR")
    return Path(env) if env else config_dir().parent / "contracts"


def _load_yaml(directory: Path, name: str) -> dict[str, Any]:
    with (directory / name).open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"{name} must contain a mapping")
    return data


@dataclass(frozen=True)
class BotParams:
    """Parameters from config/bot.yaml."""

    evening_switch_hour: int
    top_beaches: int
    wind_span_hours: int
    wind_step_hours: int
    surf_window_hours: int
    summary_top_beaches: int
    summary_stale_after_hour: int
    record_retries: int
    record_backoff_s: float
    max_age_hours: float
    cache_ttl_minutes: float
    query_timeout_s: float
    rate_limit_commands: int
    rate_limit_window_s: float
    telegram_max_chars: int
    telegram_timeout_s: float

    @classmethod
    def from_yaml(cls, data: dict[str, Any]) -> BotParams:
        """Build from the parsed bot.yaml. Optional keys fall back to documented defaults."""
        c, s, d, lim = data["commands"], data["summary"], data["data"], data["limits"]
        return cls(
            evening_switch_hour=int(c["evening_switch_local_hour"]),
            top_beaches=int(c["top_beaches"]),
            wind_span_hours=int(c["wind_span_hours"]),
            wind_step_hours=int(c["wind_step_hours"]),
            surf_window_hours=int(c.get("surf_window_hours", 24)),
            summary_top_beaches=int(s["top_beaches"]),
            summary_stale_after_hour=int(s["stale_after_local_hour"]),
            record_retries=int(s["record_retries"]),
            record_backoff_s=float(s["record_backoff_s"]),
            max_age_hours=float(d["max_age_hours"]),
            cache_ttl_minutes=float(d["cache_ttl_minutes"]),
            query_timeout_s=float(d["query_timeout_s"]),
            rate_limit_commands=int(lim["rate_limit_commands"]),
            rate_limit_window_s=float(lim["rate_limit_window_s"]),
            telegram_max_chars=int(lim["telegram_max_chars"]),
            telegram_timeout_s=float(lim.get("telegram_timeout_s", 30)),
        )


@dataclass(frozen=True)
class BotConfig:
    """The parts of config/ the bot needs."""

    alerts: dict[str, Any]
    locations: dict[str, Any]
    surf: dict[str, Any]
    params: BotParams

    @property
    def horizon_hours(self) -> int:
        return int(self.alerts["horizon_hours"])

    @property
    def hazard_labels(self) -> dict[str, str]:
        return {k: str(v["label_tr"]) for k, v in self.alerts["hazards"].items()}

    @property
    def region_names(self) -> dict[str, str]:
        return {r["id"]: r["name"] for r in self.locations["regions"]}

    @property
    def max_notification_age_minutes(self) -> float:
        return float(self.alerts["delivery"]["max_notification_age_minutes"])

    @property
    def disclaimer(self) -> str:
        return " ".join(str(self.alerts["disclaimer_tr"]).split())

    @property
    def attribution(self) -> str:
        return str(self.alerts["attribution"])

    @property
    def surf_spot_id(self) -> str:
        """First location of type surf_spot (Alaçatı)."""
        for loc in self.locations["locations"]:
            if loc.get("type") == "surf_spot":
                return str(loc["id"])
        raise ValueError("no surf_spot in locations.yaml")

    @property
    def surf_spots(self) -> list[tuple[str, str, str]]:
        """(id, name, sport) of every surf_spot location, in locations.yaml order."""
        spots = self.surf.get("spots", {})
        return [
            (str(loc["id"]), str(loc["name"]), str(spots.get(loc["id"], {}).get("sport", "")))
            for loc in self.locations["locations"]
            if loc.get("type") == "surf_spot"
        ]


def load_config(directory: Path | None = None) -> BotConfig:
    """Read alerts, locations, surf and bot YAML files."""
    d = directory or config_dir()
    return BotConfig(
        alerts=_load_yaml(d, "alerts.yaml"),
        locations=_load_yaml(d, "locations.yaml"),
        surf=_load_yaml(d, "surf.yaml"),
        params=BotParams.from_yaml(_load_yaml(d, "bot.yaml")),
    )


@dataclass(frozen=True)
class Env:
    """Environment settings."""

    project: str | None
    bq_location: str
    marts_dataset: str
    ops_dataset: str
    token: str | None
    chat_id: str | None
    broadcast_chat: str | None = None
    kafka_bootstrap: str | None = None

    @property
    def broadcast_chat_id(self) -> str | None:
        """Chat for alerts and the morning summary; falls back to TELEGRAM_CHAT_ID."""
        return self.broadcast_chat or self.chat_id


def load_settings() -> Env:
    """Read env vars (call load_env() first)."""
    g = os.environ.get
    return Env(
        project=g("GCP_PROJECT_ID"),
        bq_location=g("BQ_LOCATION", "EU") or "EU",
        marts_dataset=g("BQ_DATASET_MARTS", "alacati_marts") or "alacati_marts",
        ops_dataset=g("BQ_DATASET_OPS", "alacati_ops") or "alacati_ops",
        token=g("TELEGRAM_BOT_TOKEN"),
        chat_id=g("TELEGRAM_CHAT_ID"),
        broadcast_chat=g("TELEGRAM_BROADCAST_CHAT_ID") or None,
        kafka_bootstrap=g("KAFKA_BOOTSTRAP_SERVERS") or None,
    )
