"""Configuration loading: repo YAML files, environment and Streamlit secrets."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

APP_DIR = Path(__file__).resolve().parent
REPO_ROOT = APP_DIR.parent.parent
CONFIG_DIR = REPO_ROOT / "config"

DEFAULT_DATASET = "alacati_marts"
BQ_LOCATION = "EU"


@dataclass(frozen=True)
class AppConfig:
    """Parsed content of the config/ YAML files the dashboard needs."""

    alerts: dict[str, Any]
    locations: dict[str, Any]
    beach_score: dict[str, Any]
    surf: dict[str, Any]

    @property
    def horizon_hours(self) -> int:
        return int(self.alerts["horizon_hours"])

    @property
    def hazard_labels(self) -> dict[str, str]:
        return {k: v["label_tr"] for k, v in self.alerts["hazards"].items()}

    @property
    def region_names(self) -> dict[str, str]:
        return {r["id"]: r["name"] for r in self.locations["regions"]}

    @property
    def locations_by_id(self) -> dict[str, dict[str, Any]]:
        return {loc["id"]: loc for loc in self.locations["locations"]}

    @property
    def disclaimer(self) -> str:
        return " ".join(str(self.alerts["disclaimer_tr"]).split())

    @property
    def attribution(self) -> str:
        return str(self.alerts["attribution"])


def _load_yaml(name: str) -> dict[str, Any]:
    with (CONFIG_DIR / name).open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def load_config() -> AppConfig:
    """Read the YAML config files from the repository's config/ folder."""
    return AppConfig(
        alerts=_load_yaml("alerts.yaml"),
        locations=_load_yaml("locations.yaml"),
        beach_score=_load_yaml("beach_score.yaml"),
        surf=_load_yaml("surf.yaml"),
    )


def load_env() -> None:
    """Load the repo-root .env (no-op if missing; never overrides real env vars)."""
    load_dotenv(REPO_ROOT / ".env")


def secret_or_env(secrets: Any, key: str, default: str | None = None) -> str | None:
    """Return ``key`` from Streamlit secrets, then the environment, then ``default``."""
    try:
        if key in secrets:
            return str(secrets[key])
    except Exception:  # no secrets.toml at all
        pass
    return os.environ.get(key, default)
