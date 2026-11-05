"""morning_summary: send the Telegram morning summary at 08:00 Europe/Istanbul.

`alacati-bot-summary` is idempotent (alacati_ops.summary_log): exit 0 means "sent" or
"already sent today", so retries and a late run after the laptop wakes are safe.

Stale guard (in this DAG): a scheduled run is skipped when
  * it is already STALE_AFTER_LOCAL or later in Europe/Istanbul, or
  * the run belongs to an earlier local day (e.g. yesterday's 08:00 slot started at
    07:00 today because the laptop slept overnight; that would otherwise send today's
    summary early and block the real 08:00 run through summary_log).
Manual triggers from the UI bypass the guard on purpose.
"""

from __future__ import annotations

import logging
from datetime import time, timedelta

import pendulum
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from alacati_runtime import SUMMARY_BIN, run_cli

log = logging.getLogger(__name__)

# --- Scheduling (not weather parameters) ---------------------------------------------
LOCAL_TZ = "Europe/Istanbul"
# 08:00 local time every day (timezone-aware schedule; Istanbul has no DST, UTC+3).
SUMMARY_CRON = "0 8 * * *"
# After this local time a morning summary is stale and is not sent by scheduled runs.
STALE_AFTER_LOCAL = time(12, 0)
SUMMARY_TIMEOUT = timedelta(minutes=5)
WATCHDOG_MARGIN = timedelta(minutes=1)


def stale_reason(now_utc: pendulum.DateTime, run_after_utc: pendulum.DateTime) -> str | None:
    """Return why a scheduled summary run must not send, or None if it may send."""
    now_local = now_utc.in_timezone(LOCAL_TZ)
    slot_local = run_after_utc.in_timezone(LOCAL_TZ)
    if slot_local.date() != now_local.date():
        return f"run slot {slot_local:%Y-%m-%d %H:%M} is from an earlier local day"
    if now_local.time() >= STALE_AFTER_LOCAL:
        return f"it is {now_local:%H:%M} {LOCAL_TZ}, past {STALE_AFTER_LOCAL:%H:%M}"
    return None


@dag(
    dag_id="morning_summary",
    schedule=CronTriggerTimetable(SUMMARY_CRON, timezone=LOCAL_TZ),
    start_date=pendulum.datetime(2026, 11, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["alacati", "telegram"],
    doc_md=__doc__,
)
def morning_summary() -> None:
    @task(task_id="send_summary", execution_timeout=SUMMARY_TIMEOUT)
    def send_summary() -> None:
        dag_run = get_current_context()["dag_run"]
        run_type = str(getattr(dag_run.run_type, "value", dag_run.run_type))
        if run_type != "manual":
            run_after = pendulum.instance(dag_run.run_after)
            reason = stale_reason(pendulum.now("UTC"), run_after)
            if reason:
                log.warning("skipping stale morning summary: %s", reason)
                raise AirflowSkipException(reason)

        rc = run_cli([SUMMARY_BIN], timeout=SUMMARY_TIMEOUT - WATCHDOG_MARGIN)
        if rc != 0:
            raise RuntimeError(f"alacati-bot-summary failed with exit code {rc}")

    send_summary()


morning_summary()
