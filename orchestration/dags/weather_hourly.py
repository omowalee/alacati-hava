"""weather_hourly: Open-Meteo ingest -> dbt build -> dbt source freshness (warn-only).

Recovery after laptop downtime: catchup=False makes Airflow start only the most recent
missed run when the scheduler comes back; the ingestion itself widens `past_hours` to
"hours since the last successful run" (capped by config/ingestion.yaml max_catchup_hours),
so no hours are lost and no backlog of runs piles up.

Weather thresholds are not used here; they live in config/ and reach dbt via run_dbt.py.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import CronTriggerTimetable, dag, task
from alacati_runtime import DBT_PYTHON, INGEST_BIN, RUN_DBT, run_cli

log = logging.getLogger(__name__)

# --- Scheduling (not weather parameters) ---------------------------------------------
# Minute 5 of every hour (UTC): a few minutes past the hour, off the busy :00 mark.
HOURLY_CRON = "5 * * * *"
SCHEDULE_TIMEZONE = "UTC"
# A run must finish well before the next one starts.
DAGRUN_TIMEOUT = timedelta(minutes=55)
# Per-task hard limits (Airflow) and the slightly shorter subprocess watchdogs.
INGEST_TIMEOUT = timedelta(minutes=20)
DBT_BUILD_TIMEOUT = timedelta(minutes=30)
FRESHNESS_TIMEOUT = timedelta(minutes=10)
WATCHDOG_MARGIN = timedelta(minutes=2)

# alacati-ingest run exit codes (ingestion/src/alacati_ingestion/cli.py).
INGEST_COMPLETE = 0
INGEST_PARTIAL = 2


@dag(
    dag_id="weather_hourly",
    schedule=CronTriggerTimetable(HOURLY_CRON, timezone=SCHEDULE_TIMEZONE),
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DAGRUN_TIMEOUT,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["alacati", "ingestion", "dbt"],
    doc_md=__doc__,
)
def weather_hourly() -> None:
    @task(task_id="ingest", execution_timeout=INGEST_TIMEOUT)
    def ingest() -> str:
        rc = run_cli(
            [INGEST_BIN, "run", "--trigger", "schedule"],
            timeout=INGEST_TIMEOUT - WATCHDOG_MARGIN,
        )
        if rc == INGEST_COMPLETE:
            return "complete"
        if rc == INGEST_PARTIAL:
            # Some points failed after retries. dbt still runs: marts only use the latest
            # complete run, so a partial run never reaches the alerts.
            log.warning("ingest finished PARTIAL (exit 2): some points are missing this hour")
            return "partial"
        raise RuntimeError(f"alacati-ingest run failed with exit code {rc}")

    @task(task_id="dbt_build", execution_timeout=DBT_BUILD_TIMEOUT)
    def dbt_build() -> None:
        rc = run_cli(
            [DBT_PYTHON, RUN_DBT, "build"],
            timeout=DBT_BUILD_TIMEOUT - WATCHDOG_MARGIN,
            cwd=RUN_DBT.parent,
        )
        if rc != 0:
            raise RuntimeError(f"dbt build failed with exit code {rc}")

    @task(task_id="dbt_source_freshness", execution_timeout=FRESHNESS_TIMEOUT, retries=0)
    def dbt_source_freshness() -> str:
        """Warn-only: a stale or failing freshness check never fails the DAG run."""
        try:
            rc = run_cli(
                [DBT_PYTHON, RUN_DBT, "source", "freshness"],
                timeout=FRESHNESS_TIMEOUT - WATCHDOG_MARGIN,
                cwd=RUN_DBT.parent,
            )
        except Exception:
            log.warning("dbt source freshness could not run", exc_info=True)
            return "error"
        if rc != 0:
            log.warning("dbt source freshness reported stale sources or failed (exit %s)", rc)
            return "stale"
        return "fresh"

    ingest() >> dbt_build() >> dbt_source_freshness()


weather_hourly()
