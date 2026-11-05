"""morning_summary: send the Telegram morning summary at 08:00 Europe/Istanbul.

`alacati-bot-summary` is idempotent (alacati_ops.summary_log). Exit codes:
  * 0: sent, or already sent today            -> success
  * 3: sent, but the summary_log write failed -> success with a warning. Never retried:
       a retry would not find the log row and would send the summary a second time.
  * anything else (1 = error, 124 = watchdog)  -> failure, retried.

Stale guard (in this DAG): a scheduled run is skipped when
  * it is already config/bot.yaml `summary.stale_after_local_hour` or later in
    Europe/Istanbul, or
  * the run belongs to an earlier local day (e.g. yesterday's 08:00 slot started at
    07:00 today because the laptop slept overnight; that would otherwise send today's
    summary early and block the real 08:00 run through summary_log).
Manual triggers from the UI bypass the guard on purpose.
"""

from __future__ import annotations

import logging
from datetime import time, timedelta

import pendulum
import yaml
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from alacati_runtime import CONFIG_DIR, SUMMARY_BIN, notify_failure, run_cli

log = logging.getLogger(__name__)

# --- Scheduling (not weather parameters) ---------------------------------------------
LOCAL_TZ = "Europe/Istanbul"
# 08:00 local time every day (timezone-aware schedule; Istanbul has no DST, UTC+3).
SUMMARY_CRON = "0 8 * * *"
BOT_CONFIG = CONFIG_DIR / "bot.yaml"

# alacati-bot-summary exit codes (apps/telegram_bot/src/alacati_bot/summary.py).
SUMMARY_OK = 0
SUMMARY_SENT_LOG_FAILED = 3

# Time budget. Worst case of one run: RETRIES + 1 attempts of SUMMARY_TIMEOUT plus
# RETRIES retry delays = 4 * 5 + 3 * 5 = 35 min. DAGRUN_TIMEOUT sits just above that, so
# it never cuts a legitimate retry short and only catches a run stuck in the scheduler.
SUMMARY_TIMEOUT = timedelta(minutes=5)
WATCHDOG_MARGIN = timedelta(minutes=1)
RETRIES = 3
RETRY_DELAY = timedelta(minutes=5)
DAGRUN_TIMEOUT = timedelta(minutes=45)
_WORST_CASE = (RETRIES + 1) * SUMMARY_TIMEOUT + RETRIES * RETRY_DELAY
if DAGRUN_TIMEOUT < _WORST_CASE:  # fail at parse time (import error) if edited wrongly
    raise ValueError(f"DAGRUN_TIMEOUT {DAGRUN_TIMEOUT} < worst-case task time {_WORST_CASE}")


def stale_after_local() -> time:
    """config/bot.yaml summary.stale_after_local_hour (read at run time, no rebuild)."""
    with BOT_CONFIG.open(encoding="utf-8") as fh:
        hour = int(yaml.safe_load(fh)["summary"]["stale_after_local_hour"])
    if not 0 <= hour <= 23:
        raise ValueError(f"{BOT_CONFIG}: summary.stale_after_local_hour={hour} is not 0..23")
    return time(hour, 0)


def stale_reason(
    now_utc: pendulum.DateTime, run_after_utc: pendulum.DateTime, stale_after: time
) -> str | None:
    """Return why a scheduled summary run must not send, or None if it may send."""
    now_local = now_utc.in_timezone(LOCAL_TZ)
    slot_local = run_after_utc.in_timezone(LOCAL_TZ)
    if slot_local.date() != now_local.date():
        return f"run slot {slot_local:%Y-%m-%d %H:%M} is from an earlier local day"
    if now_local.time() >= stale_after:
        return f"it is {now_local:%H:%M} {LOCAL_TZ}, past {stale_after:%H:%M}"
    return None


@dag(
    dag_id="morning_summary",
    schedule=CronTriggerTimetable(SUMMARY_CRON, timezone=LOCAL_TZ),
    start_date=pendulum.datetime(2026, 11, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DAGRUN_TIMEOUT,
    default_args={"retries": RETRIES, "retry_delay": RETRY_DELAY},
    # One Telegram message per failed run (after retries), sent from the dag-processor.
    on_failure_callback=notify_failure,
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
            reason = stale_reason(pendulum.now("UTC"), run_after, stale_after_local())
            if reason:
                log.warning("skipping stale morning summary: %s", reason)
                raise AirflowSkipException(reason)

        rc = run_cli([SUMMARY_BIN], timeout=SUMMARY_TIMEOUT - WATCHDOG_MARGIN)
        if rc == SUMMARY_OK:
            return
        if rc == SUMMARY_SENT_LOG_FAILED:
            # Success on purpose: the message is out. Retrying would send it again.
            log.warning(
                "morning summary SENT, but writing alacati_ops.summary_log failed (exit 3); "
                "not retrying. A later manual trigger today would send it again."
            )
            return
        raise RuntimeError(f"alacati-bot-summary failed with exit code {rc}")

    send_summary()


morning_summary()
