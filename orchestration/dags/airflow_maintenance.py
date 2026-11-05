"""airflow_maintenance: weekly housekeeping for a months-long laptop uptime.

* `db_clean`: `airflow db clean` for metadata rows older than RETENTION (dag runs, task
  instances, logs, xcom, jobs, ...), without archive tables.
* `prune_task_logs`: deletes task log files older than RETENTION from the airflow-logs
  volume, then removes the empty folders left behind.

Airflow 3 blocks metadata-DB access from task processes (it rewrites
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN to a dummy URL). `airflow db clean` is an admin CLI,
so it runs as a subprocess that gets the real URL from ALACATI_MAINTENANCE_DB_CONN, which
docker-compose.yml sets on the scheduler only. That variable is stripped from every other
component subprocess (alacati_runtime.child_env).
"""

from __future__ import annotations

import logging
import os
import shutil
import time
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import CronTriggerTimetable, dag, task
from alacati_runtime import MAINTENANCE_DB_CONN_VAR, notify_failure, run_cli

log = logging.getLogger(__name__)

# --- Scheduling / retention (operational, not weather parameters) ----------------------
# Sundays 02:30 UTC (05:30 Istanbul): far from the 08:00 summary, between hourly ingests.
MAINTENANCE_CRON = "30 2 * * 0"
RETENTION = timedelta(days=30)
LOG_DIR = Path(os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs"))
DB_CLEAN_TIMEOUT = timedelta(minutes=20)
PRUNE_TIMEOUT = timedelta(minutes=10)
WATCHDOG_MARGIN = timedelta(minutes=2)
# Worst case: 20 + 10 min, no retries; the run timeout leaves room for scheduling.
DAGRUN_TIMEOUT = timedelta(minutes=45)
if DAGRUN_TIMEOUT < DB_CLEAN_TIMEOUT + PRUNE_TIMEOUT:
    raise ValueError("DAGRUN_TIMEOUT must cover DB_CLEAN_TIMEOUT + PRUNE_TIMEOUT")

# Variables of the task process that must not reach the `airflow db clean` subprocess.
_DROP_FOR_ADMIN_CLI = (
    "AIRFLOW__CORE__SQL_ALCHEMY_CONN",  # deprecated alias, also set to the blocked URL
    "_AIRFLOW_PROCESS_CONTEXT",
    "__AIRFLOW_SUPERVISOR_FD",
)


@dag(
    dag_id="airflow_maintenance",
    schedule=CronTriggerTimetable(MAINTENANCE_CRON, timezone="UTC"),
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DAGRUN_TIMEOUT,
    # Weekly and harmless to skip once: no retries, a failure is reported by Telegram.
    default_args={"retries": 0},
    on_failure_callback=notify_failure,
    tags=["alacati", "maintenance"],
    doc_md=__doc__,
)
def airflow_maintenance() -> None:
    @task(task_id="db_clean", execution_timeout=DB_CLEAN_TIMEOUT)
    def db_clean() -> None:
        db_conn = os.environ.get(MAINTENANCE_DB_CONN_VAR)
        if not db_conn:
            raise RuntimeError(f"{MAINTENANCE_DB_CONN_VAR} is not set on this container")
        env = {k: v for k, v in os.environ.items() if k not in _DROP_FOR_ADMIN_CLI}
        env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = db_conn
        cutoff = pendulum.now("UTC") - RETENTION
        rc = run_cli(
            [
                "airflow", "db", "clean",
                "--clean-before-timestamp", cutoff.format("YYYY-MM-DD HH:mm:ssZ"),
                "--yes", "--skip-archive",
            ],
            timeout=DB_CLEAN_TIMEOUT - WATCHDOG_MARGIN,
            env=env,
        )
        if rc != 0:
            raise RuntimeError(f"airflow db clean failed with exit code {rc}")

    @task(task_id="prune_task_logs", execution_timeout=PRUNE_TIMEOUT)
    def prune_task_logs() -> dict[str, int]:
        cutoff = time.time() - RETENTION.total_seconds()
        removed_files = removed_bytes = removed_dirs = 0
        if not LOG_DIR.is_dir():
            log.warning("log folder %s does not exist; nothing to prune", LOG_DIR)
            return {"files": 0, "bytes": 0, "dirs": 0}
        for path in LOG_DIR.rglob("*"):
            try:
                if path.is_file() and not path.is_symlink() and path.stat().st_mtime < cutoff:
                    size = path.stat().st_size
                    path.unlink()
                    removed_files += 1
                    removed_bytes += size
            except OSError as exc:  # a file vanished or is not ours: keep going
                log.warning("could not remove %s: %s", path, exc)
        # Deepest folders first, so parents empty out in the same pass. LOG_DIR stays.
        # Runs at 02:30 UTC, away from the hourly :05 tasks, so no task is creating a
        # fresh (still empty) log folder at this moment.
        for folder in sorted((p for p in LOG_DIR.rglob("*") if p.is_dir()), reverse=True):
            try:
                if not any(folder.iterdir()):
                    folder.rmdir()
                    removed_dirs += 1
            except OSError as exc:
                log.warning("could not remove folder %s: %s", folder, exc)
        usage = shutil.disk_usage(LOG_DIR)
        log.info(
            "pruned %d log files (%.1f MB) and %d empty folders older than %s; "
            "volume free: %.1f GB",
            removed_files, removed_bytes / 1e6, removed_dirs, RETENTION, usage.free / 1e9,
        )
        return {"files": removed_files, "bytes": removed_bytes, "dirs": removed_dirs}

    # Independent tasks; both run even if the other fails.
    db_clean()
    prune_task_logs()


airflow_maintenance()
