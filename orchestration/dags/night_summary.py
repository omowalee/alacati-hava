"""night_summary: send the Telegram night summary at 21:30 Europe/Istanbul.

Rain, wind and alerts from the next full hour until config/bot.yaml
`night.end_local_hour` (08:00) the next morning, or "sakin bir gece". Waits for fresh
forecast data first, then sends once per local day; see summary_dag.py for the tasks,
exit codes and the stale guard. A scheduled run from an earlier local day (laptop asleep
past midnight) is skipped; `alacati-bot-summary` itself never sends a night summary
before `night.earliest_local_hour`.
"""

from __future__ import annotations

from airflow.sdk import DAG
from summary_dag import build_summary_dag

# 21:30 local time every day (timezone-aware schedule; Istanbul has no DST, UTC+3).
NIGHT_CRON = "30 21 * * *"

dag: DAG = build_summary_dag(
    dag_id="night_summary",
    kind="night",
    cron=NIGHT_CRON,
    doc_md=__doc__ or "",
    stale_after=lambda: None,
)
