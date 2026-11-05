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


def _load_yaml(directory: Path, name: str) -> dict[str, Any]:
    with (directory / name).open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"{name} must contain a mapping")
    return data


@dataclass(frozen=True)
class BotConfig:
    """The parts of config/ the bot needs."""

    alerts: dict[str, Any]
    locations: dict[str, Any]
    surf: dict[str, Any]

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


def load_config(directory: Path | None = None) -> BotConfig:
    """Read alerts, locations and surf YAML files."""
    d = directory or config_dir()
    return BotConfig(
        alerts=_load_yaml(d, "alerts.yaml"),
        locations=_load_yaml(d, "locations.yaml"),
        surf=_load_yaml(d, "surf.yaml"),
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
    )
