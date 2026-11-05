"""Run dbt with the repo's config/*.yaml files injected as `--vars`.

Usage (from the dbt/ folder):
    uv run python run_dbt.py build
    uv run python run_dbt.py build --select mart_alert_events
    uv run python run_dbt.py source freshness

Thresholds and parameters live in config/ (CLAUDE.md rule 3); the SQL models read them
through var('alerts'), var('beach_score'), var('surf'), var('locations'), ...
Every argument is forwarded to dbt unchanged.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

DBT_DIR = Path(__file__).resolve().parent
REPO_ROOT = DBT_DIR.parent
CONFIG_DIR = REPO_ROOT / "config"

# Commands that do not accept --vars.
NO_VARS_COMMANDS = {"clean", "deps", "init", "--version", "-h", "--help"}


def _load_yaml(name: str) -> dict:
    with (CONFIG_DIR / name).open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def build_vars() -> dict:
    locations_cfg = _load_yaml("locations.yaml")
    return {
        "regions": locations_cfg["regions"],
        "marine_zones": locations_cfg["marine_zones"],
        "locations": locations_cfg["locations"],
        "alerts": _load_yaml("alerts.yaml"),
        "beach_score": _load_yaml("beach_score.yaml"),
        "surf": _load_yaml("surf.yaml"),
    }


def main(argv: list[str]) -> int:
    load_dotenv(REPO_ROOT / ".env")
    os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))
    os.environ.setdefault("BQ_DATASET_RAW", "alacati_raw")
    os.environ.setdefault("BQ_DATASET_OPS", "alacati_ops")
    os.environ.setdefault("BQ_DATASET_STAGING", "alacati_staging")
    os.environ.setdefault("BQ_DATASET_MARTS", "alacati_marts")
    os.environ.setdefault("BQ_LOCATION", "EU")
    if "GCP_PROJECT_ID" not in os.environ:
        print("GCP_PROJECT_ID is not set (expected in the repo-root .env).", file=sys.stderr)
        return 2

    args = list(argv)
    if "--project-dir" not in args:
        args += ["--project-dir", str(DBT_DIR)]
    if args and args[0] not in NO_VARS_COMMANDS and "--vars" not in args:
        # ensure_ascii keeps the command line pure ASCII (Turkish labels become \uXXXX).
        args += ["--vars", json.dumps(build_vars(), ensure_ascii=True, separators=(",", ":"))]

    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke(args)
    if result.exception is not None:
        print(result.exception, file=sys.stderr)
        return 2
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
