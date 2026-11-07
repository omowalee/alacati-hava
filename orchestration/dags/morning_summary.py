"""morning_summary: send the Telegram morning summary at 08:00 Europe/Istanbul.

Waits for fresh forecast data first, then sends once per local day; see summary_dag.py
for the tasks, exit codes and the stale guard. Scheduled runs are additionally skipped
from config/bot.yaml `summary.stale_after_local_hour` on (a missed 08:00 summary is not
worth sending in the afternoon).
"""

from __future__ import annotations

from datetime import time

import yaml
from airflow.sdk import DAG
from alacati_runtime import CONFIG_DIR
from summary_dag import build_summary_dag

# 08:00 local time every day (timezone-aware schedule; Istanbul has no DST, UTC+3).
SUMMARY_CRON = "0 8 * * *"
BOT_CONFIG = CONFIG_DIR / "bot.yaml"


def stale_after_local() -> time:
    """config/bot.yaml summary.stale_after_local_hour (read at run time, no rebuild)."""
    with BOT_CONFIG.open(encoding="utf-8") as fh:
        hour = int(yaml.safe_load(fh)["summary"]["stale_after_local_hour"])
    if not 0 <= hour <= 23:
        raise ValueError(f"{BOT_CONFIG}: summary.stale_after_local_hour={hour} is not 0..23")
    return time(hour, 0)


dag: DAG = build_summary_dag(
    dag_id="morning_summary",
    kind="morning",
    cron=SUMMARY_CRON,
    doc_md=__doc__ or "",
    stale_after=stale_after_local,
)
