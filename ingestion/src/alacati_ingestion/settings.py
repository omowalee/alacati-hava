"""Paths, environment and YAML config loading shared by all ingestion modules."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_DIR = REPO_ROOT / "config"
CONTRACTS_DIR = REPO_ROOT / "contracts"

load_dotenv(REPO_ROOT / ".env")


@dataclass(frozen=True)
class BigQuerySettings:
    project_id: str
    location: str
    datasets: dict[str, str]  # logical name (raw, ops, staging, marts) -> dataset id


def bigquery_settings() -> BigQuerySettings:
    project = os.environ.get("GCP_PROJECT_ID")
    if not project:
        raise RuntimeError("GCP_PROJECT_ID is not set (copy .env.example to .env)")
    return BigQuerySettings(
        project_id=project,
        location=os.environ.get("BQ_LOCATION", "EU"),
        datasets={
            "raw": os.environ.get("BQ_DATASET_RAW", "alacati_raw"),
            "ops": os.environ.get("BQ_DATASET_OPS", "alacati_ops"),
            "staging": os.environ.get("BQ_DATASET_STAGING", "alacati_staging"),
            "marts": os.environ.get("BQ_DATASET_MARTS", "alacati_marts"),
        },
    )


@cache
def load_config(name: str) -> dict[str, Any]:
    """Load config/<name>.yaml."""
    with (CONFIG_DIR / f"{name}.yaml").open(encoding="utf-8") as fh:
        data: dict[str, Any] = yaml.safe_load(fh)
    return data
