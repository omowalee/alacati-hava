"""weather_hourly: ingest (-> Kafka) -> wait_raw_loaded -> dbt build -> dbt source freshness.

Phase 3 data flow: `alacati-ingest run` (INGEST_SINK=kafka, set in docker-compose.yml)
publishes one weather.raw message per point and writes ops.ingest_runs / api_call_log;
the `bq-consumer` service loads the raw tables. `wait_raw_loaded` polls BigQuery until
every published message of this run is in the raw tables, so dbt never builds on a
half-loaded run. The `alert-engine` service then picks up the run from Kafka, waits for
this dbt build and sends the alert lifecycle to weather.alerts.

Recovery after laptop downtime: catchup=False makes Airflow start only the most recent
missed run when the scheduler comes back; the ingestion itself widens `past_hours` to
"hours since the last successful run" (capped by config/ingestion.yaml max_catchup_hours),
so no hours are lost and no backlog of runs piles up. Kafka keeps weather.raw for 7 days
and the consumers resume from their committed offsets.

Weather thresholds are not used here; they live in config/ and reach dbt via run_dbt.py.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.sdk import CronTriggerTimetable, dag, task
from alacati_runtime import (
    DBT_PYTHON,
    INGEST_BIN,
    INGEST_PYTHON,
    RUN_DBT,
    notify_failure,
    run_cli,
)

log = logging.getLogger(__name__)

# --- Scheduling (not weather parameters) ---------------------------------------------
# Minute 5 of every hour (UTC): a few minutes past the hour, off the busy :00 mark.
HOURLY_CRON = "5 * * * *"
SCHEDULE_TIMEZONE = "UTC"
# Time budget. A run must finish before the next hourly slot, so DAGRUN_TIMEOUT is 55 min,
# and the per-task limits are sized so that the worst case still fits inside it:
#   ingest           2 attempts * 7 min + 1 retry delay of 2 min  = 16 min
#   wait_raw_loaded  1 attempt  * 11 min (sensor timeout 10 min)  = 11 min
#   dbt_build        2 attempts * 10 min + 1 retry delay of 2 min = 22 min
#   freshness        1 attempt  * 4 min (retries=0)               =  4 min
#   total                                                          = 53 min
# Typical durations (Nov 2026): ingest ~20 s, raw load by the bq-consumer ~10 s after
# the publish, dbt build ~1 min, freshness ~5 s, so the limits are still >= 5x the normal
# run time. The check below fails DAG parsing (import error in the UI) if someone raises
# a task limit without raising DAGRUN_TIMEOUT.
DAGRUN_TIMEOUT = timedelta(minutes=55)
RETRIES = 1
RETRY_DELAY = timedelta(minutes=2)
# Per-task hard limits (Airflow) and the slightly shorter subprocess watchdogs.
INGEST_TIMEOUT = timedelta(minutes=7)
DBT_BUILD_TIMEOUT = timedelta(minutes=10)
FRESHNESS_TIMEOUT = timedelta(minutes=4)
WATCHDOG_MARGIN = timedelta(minutes=2)
# wait_raw_loaded: poke every 15 s; fail (-> failure notification) after 10 min. A
# sensor timeout is never retried, so it counts once in the budget.
RAW_POKE_INTERVAL = timedelta(seconds=15)
RAW_WAIT_TIMEOUT = timedelta(minutes=10)
RAW_WAIT_TASK_TIMEOUT = RAW_WAIT_TIMEOUT + timedelta(minutes=1)
RAW_CHECK_TIMEOUT = timedelta(minutes=1)  # one BigQuery check (subprocess watchdog)
_WORST_CASE = (
    (RETRIES + 1) * (INGEST_TIMEOUT + DBT_BUILD_TIMEOUT)
    + 2 * RETRIES * RETRY_DELAY
    + RAW_WAIT_TASK_TIMEOUT
    + FRESHNESS_TIMEOUT
)
if DAGRUN_TIMEOUT < _WORST_CASE:
    raise ValueError(f"DAGRUN_TIMEOUT {DAGRUN_TIMEOUT} < worst-case task time {_WORST_CASE}")

# alacati-ingest run exit codes (ingestion/src/alacati_ingestion/cli.py).
INGEST_COMPLETE = 0
INGEST_PARTIAL = 2
# Last stdout line of `alacati-ingest run`.
RUN_ID_PREFIX = "RUN_ID="

# scripts/check_raw_loaded.py (run with the ingestion venv) exit codes.
CHECK_RAW_SCRIPT = Path(__file__).resolve().parent / "scripts" / "check_raw_loaded.py"
RAW_LOADED = 0
RAW_NOT_YET = 10


def parse_run_id(lines: list[str]) -> str | None:
    """The run id from the last `RUN_ID=<uuid>` line of the ingest output."""
    for line in reversed(lines):
        if line.startswith(RUN_ID_PREFIX):
            return line[len(RUN_ID_PREFIX) :].strip() or None
    return None


@dag(
    dag_id="weather_hourly",
    schedule=CronTriggerTimetable(HOURLY_CRON, timezone=SCHEDULE_TIMEZONE),
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DAGRUN_TIMEOUT,
    default_args={"retries": RETRIES, "retry_delay": RETRY_DELAY},
    # One Telegram message per failed run (after retries), sent from the dag-processor.
    # Ingest exit 2 (partial) is not a failure and sends nothing.
    on_failure_callback=notify_failure,
    tags=["alacati", "ingestion", "kafka", "dbt"],
    doc_md=__doc__,
)
def weather_hourly() -> None:
    @task(task_id="ingest", execution_timeout=INGEST_TIMEOUT)
    def ingest() -> dict[str, Any]:
        """Returns {"status": complete|partial, "run_id": <uuid> | None} (XCom)."""
        lines: list[str] = []
        rc = run_cli(
            [INGEST_BIN, "run", "--trigger", "schedule"],
            timeout=INGEST_TIMEOUT - WATCHDOG_MARGIN,
            on_line=lines.append,
        )
        if rc not in (INGEST_COMPLETE, INGEST_PARTIAL):
            raise RuntimeError(f"alacati-ingest run failed with exit code {rc}")
        run_id = parse_run_id(lines)
        if run_id is None:
            if os.environ.get("INGEST_SINK", "bigquery") == "kafka":
                raise RuntimeError("alacati-ingest run printed no RUN_ID= line")
            # Direct BigQuery sink (fallback / older image): raw rows are already loaded.
            log.warning("no RUN_ID= line; INGEST_SINK is not kafka, so nothing to wait for")
        if rc == INGEST_PARTIAL:
            # Some points failed after retries. dbt still runs: marts only use the latest
            # complete run, so a partial run never reaches the alerts.
            log.warning("ingest finished PARTIAL (exit 2): some points are missing this hour")
            return {"status": "partial", "run_id": run_id}
        return {"status": "complete", "run_id": run_id}

    @task.sensor(
        task_id="wait_raw_loaded",
        poke_interval=RAW_POKE_INTERVAL.total_seconds(),
        timeout=RAW_WAIT_TIMEOUT.total_seconds(),
        mode="poke",
        execution_timeout=RAW_WAIT_TASK_TIMEOUT,
        retries=0,
    )
    def wait_raw_loaded(ingest_result: dict[str, Any]) -> bool:
        """True once the bq-consumer has loaded every published message of the run."""
        if not ingest_result.get("run_id"):
            return True  # direct BigQuery sink: loaded synchronously by the ingest task
        run_id = str(ingest_result["run_id"])
        rc = run_cli(
            [INGEST_PYTHON, "-I", CHECK_RAW_SCRIPT, run_id],
            timeout=RAW_CHECK_TIMEOUT,
        )
        if rc == RAW_LOADED:
            return True
        if rc != RAW_NOT_YET:
            # A transient BigQuery / credentials error: keep poking until the timeout.
            log.warning("raw-load check failed (exit %s); will retry", rc)
        return False

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

    wait_raw_loaded(ingest()) >> dbt_build() >> dbt_source_freshness()


weather_hourly()
