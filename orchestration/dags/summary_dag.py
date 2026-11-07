"""Shared builder for the Telegram summary DAGs (not a DAG file; listed in .airflowignore).

Each summary DAG has two tasks:

  wait_fresh    sensor: waits until the newest forecast in the marts was fetched at most
                config/bot.yaml `summary.fresh_max_age_minutes` ago. After the laptop slept,
                the hourly ingest runs at the same moment as the summary; without this wait
                the summary was built from hours-old data ("veriler güncel değil"). If the
                data never gets fresh, the sensor gives up (skipped, not failed) and the
                summary is still sent, with the "veri eski" warning.
  send_summary  `alacati-bot-summary --kind <kind>`, idempotent through
                alacati_ops.summary_log (one message per kind per local day). Exit codes:
                  * 0: sent, already sent today, or outside the kind's hours -> success
                  * 3: sent, but the summary_log write failed -> success with a warning.
                       Never retried: a retry would not find the log row and send again.
                  * anything else (1 = error, 124 = watchdog) -> failure, retried.

Stale guard (scheduled runs only; manual triggers bypass it on purpose): a run is skipped
when its slot belongs to an earlier local day (the laptop slept through it), or, if the
kind has one, when it is already past its `stale_after` local hour.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import time, timedelta

import pendulum
from airflow.sdk import DAG, CronTriggerTimetable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from alacati_runtime import SUMMARY_BIN, notify_failure, run_cli

log = logging.getLogger(__name__)

LOCAL_TZ = "Europe/Istanbul"

# alacati-bot-summary exit codes (apps/telegram_bot/src/alacati_bot/summary.py).
SUMMARY_OK = 0
SUMMARY_SENT_LOG_FAILED = 3
FRESH_OK = 0  # --check-fresh: 0 fresh, 4 not fresh yet, anything else = error

# Time budget. wait_fresh pokes every 2 min for up to 30 min (a sensor timeout is never
# retried; it is a soft fail, so the summary still goes out). send_summary worst case:
# RETRIES + 1 attempts of SUMMARY_TIMEOUT plus RETRIES retry delays = 4 * 5 + 3 * 5 = 35 min.
# DAGRUN_TIMEOUT sits just above the sum, so it never cuts a legitimate retry short.
FRESH_POKE_INTERVAL = timedelta(minutes=2)
FRESH_WAIT_TIMEOUT = timedelta(minutes=30)
FRESH_CHECK_TIMEOUT = timedelta(minutes=1)  # one BigQuery check (subprocess watchdog)
SUMMARY_TIMEOUT = timedelta(minutes=5)
WATCHDOG_MARGIN = timedelta(minutes=1)
RETRIES = 3
RETRY_DELAY = timedelta(minutes=5)
DAGRUN_TIMEOUT = timedelta(minutes=75)
_WORST_CASE = (
    FRESH_WAIT_TIMEOUT
    + FRESH_POKE_INTERVAL
    + (RETRIES + 1) * SUMMARY_TIMEOUT
    + RETRIES * RETRY_DELAY
)
if DAGRUN_TIMEOUT < _WORST_CASE:  # fail at parse time (import error) if edited wrongly
    raise ValueError(f"DAGRUN_TIMEOUT {DAGRUN_TIMEOUT} < worst-case task time {_WORST_CASE}")


def stale_reason(
    now_utc: pendulum.DateTime, run_after_utc: pendulum.DateTime, stale_after: time | None
) -> str | None:
    """Return why a scheduled summary run must not send, or None if it may send."""
    now_local = now_utc.in_timezone(LOCAL_TZ)
    slot_local = run_after_utc.in_timezone(LOCAL_TZ)
    if slot_local.date() != now_local.date():
        return f"run slot {slot_local:%Y-%m-%d %H:%M} is from an earlier local day"
    if stale_after is not None and now_local.time() >= stale_after:
        return f"it is {now_local:%H:%M} {LOCAL_TZ}, past {stale_after:%H:%M}"
    return None


def _guard(stale_after: Callable[[], time | None]) -> str | None:
    """stale_reason for the current run, or None for manual triggers."""
    dag_run = get_current_context()["dag_run"]
    run_type = str(getattr(dag_run.run_type, "value", dag_run.run_type))
    if run_type == "manual":
        return None
    run_after = pendulum.instance(dag_run.run_after)
    return stale_reason(pendulum.now("UTC"), run_after, stale_after())


def build_summary_dag(
    *,
    dag_id: str,
    kind: str,
    cron: str,
    doc_md: str,
    stale_after: Callable[[], time | None],
) -> DAG:
    """Create the DAG for one summary kind ('morning' or 'night')."""

    @dag(
        dag_id=dag_id,
        schedule=CronTriggerTimetable(cron, timezone=LOCAL_TZ),
        start_date=pendulum.datetime(2026, 11, 1, tz=LOCAL_TZ),
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=DAGRUN_TIMEOUT,
        default_args={"retries": RETRIES, "retry_delay": RETRY_DELAY},
        # One Telegram message per failed run (after retries), sent from the dag-processor.
        on_failure_callback=notify_failure,
        tags=["alacati", "telegram"],
        doc_md=doc_md,
    )
    def summary_dag() -> None:
        @task.sensor(
            task_id="wait_fresh",
            poke_interval=FRESH_POKE_INTERVAL.total_seconds(),
            timeout=FRESH_WAIT_TIMEOUT.total_seconds(),
            mode="reschedule",
            soft_fail=True,  # timeout -> skipped; send_summary still runs (none_failed)
            retries=0,
        )
        def wait_fresh() -> bool:
            if _guard(stale_after):
                return True  # send_summary skips this run itself; no point in waiting
            try:
                rc = run_cli([SUMMARY_BIN, "--check-fresh"], timeout=FRESH_CHECK_TIMEOUT)
            except Exception:  # never fail here: a failed sensor would block send_summary
                log.exception("freshness check could not run; waiting")
                return False
            if rc == FRESH_OK:
                return True
            # 4 = not fresh yet; anything else is a transient error. Keep poking.
            log.info("forecast data not fresh yet (exit %s); waiting", rc)
            return False

        @task(
            task_id="send_summary",
            execution_timeout=SUMMARY_TIMEOUT,
            trigger_rule="none_failed",
        )
        def send_summary() -> None:
            reason = _guard(stale_after)
            if reason:
                log.warning("skipping stale %s summary: %s", kind, reason)
                raise AirflowSkipException(reason)
            rc = run_cli(
                [SUMMARY_BIN, "--kind", kind], timeout=SUMMARY_TIMEOUT - WATCHDOG_MARGIN
            )
            if rc == SUMMARY_OK:
                return
            if rc == SUMMARY_SENT_LOG_FAILED:
                # Success on purpose: the message is out. Retrying would send it again.
                log.warning(
                    "%s summary SENT, but writing alacati_ops.summary_log failed (exit 3); "
                    "not retrying. A later manual trigger today would send it again.",
                    kind,
                )
                return
            raise RuntimeError(f"alacati-bot-summary --kind {kind} failed with exit code {rc}")

        wait_fresh() >> send_summary()

    # Module-level DAG files must mention 'airflow' and 'dag' (DAG discovery safe mode),
    # which is why morning_summary.py / night_summary.py import DAG for the annotation.
    return summary_dag()
