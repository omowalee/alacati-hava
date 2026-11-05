"""Command line: alacati-ingest run | setup.

Exit codes of `run`: 0 = complete, 2 = partial (some points missing), 1 = failed or any
ops write (ingest_runs / api_call_log) failed. `setup`: 0 = ok, 1 = problems.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

import httpx
from google.cloud import bigquery

from alacati_ingestion import bq_setup
from alacati_ingestion.bq_loader import BigQueryStore
from alacati_ingestion.open_meteo import ApiConfig, OpenMeteoClient, points_from_config
from alacati_ingestion.runner import RunResult, RunStore, execute_run
from alacati_ingestion.settings import bigquery_settings, load_config

log = logging.getLogger("alacati_ingestion")


def _print_summary(result: RunResult, dry_run: bool) -> None:
    s = result.summary
    print(f"run_id:      {s.run_id}")
    print(f"trigger:     {s.trigger}")
    print(f"past_hours:  {s.past_hours}")
    print(
        f"messages:    {s.succeeded_messages}/{s.expected_messages} ok, {s.failed_messages} failed"
    )
    weighted = sum(c.weighted_calls for c in result.calls)
    print(f"http calls:  {len(result.calls)}, weighted {weighted:.1f}")
    print(f"status:      {s.status}")
    if result.note:
        print(f"note:        {result.note}")
    if dry_run:
        print("DRY RUN: nothing written to BigQuery, budget check skipped.")


def cmd_run(trigger: str, dry_run: bool) -> int:
    config = ApiConfig.from_config(load_config("ingestion"))
    points = points_from_config(load_config("locations"))
    store: RunStore | None = None
    if not dry_run:
        settings = bigquery_settings()
        store = BigQueryStore(
            bigquery.Client(project=settings.project_id, location=settings.location), settings
        )
    with httpx.Client() as http:
        client = OpenMeteoClient(config, http)
        result = execute_run(
            config=config, points=points, client=client, store=store, cli_trigger=trigger
        )
    _print_summary(result, dry_run)
    return exit_code(result)


def exit_code(result: RunResult) -> int:
    """0 complete, 2 partial, 1 failed or ops write failure."""
    if result.ops_failed:
        log.error("ops writes failed: exiting non-zero")
        return 1
    return {"complete": 0, "partial": 2}.get(result.summary.status, 1)


def cmd_setup() -> int:
    problems = bq_setup.setup()
    for problem in problems:
        log.error(problem)
    return 1 if problems else 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="alacati-ingest")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser(
        "run",
        help="fetch Open-Meteo data and load it into BigQuery "
        "(exit codes: 0 complete, 2 partial, 1 failed or ops write failure)",
        description="Exit codes: 0 = complete, 2 = partial, 1 = failed or ops write failure.",
    )
    run.add_argument("--trigger", choices=["schedule", "manual"], default="manual")
    run.add_argument("--dry-run", action="store_true", help="fetch and validate only")
    sub.add_parser("setup", help="create BigQuery datasets and tables")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # httpx logs every request URL at INFO; our own per-run call summary is enough.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.command == "setup":
        return cmd_setup()
    return cmd_run(args.trigger, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
