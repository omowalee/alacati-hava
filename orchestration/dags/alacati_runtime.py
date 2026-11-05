"""Shared helpers for the alacati-hava DAGs (not a DAG file; listed in .airflowignore).

Every component (ingestion, dbt, bot) runs as a subprocess from its own isolated uv venv
under /opt/venvs, never inside Airflow's Python environment. Output is streamed line by
line into the Airflow task log.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# Locations inside the custom Airflow image (docker/airflow/Dockerfile).
APP_ROOT = Path(os.environ.get("ALACATI_APP_ROOT", "/opt/alacati"))
VENVS = Path(os.environ.get("ALACATI_VENVS", "/opt/venvs"))

INGEST_BIN = VENVS / "ingestion" / "bin" / "alacati-ingest"
INGEST_PYTHON = VENVS / "ingestion" / "bin" / "python"
DBT_PYTHON = VENVS / "dbt" / "bin" / "python"
RUN_DBT = APP_ROOT / "dbt" / "run_dbt.py"
SUMMARY_BIN = VENVS / "bot" / "bin" / "alacati-bot-summary"
CONFIG_DIR = Path(os.environ.get("ALACATI_CONFIG_DIR", "/opt/alacati/config"))

# Real metadata-DB URL for the maintenance DAG only (set on the scheduler in
# docker-compose.yml). Airflow 3 replaces AIRFLOW__DATABASE__SQL_ALCHEMY_CONN with a
# blocked URL inside task processes, so `airflow db clean` needs it under another name.
MAINTENANCE_DB_CONN_VAR = "ALACATI_MAINTENANCE_DB_CONN"

# Environment variables that belong to Airflow itself and must not leak into the
# component processes (DB connection string, fernet key, JWT secret, ...).
_STRIP_PREFIXES = ("AIRFLOW__", "_AIRFLOW", "AIRFLOW_CTX_")
_STRIP_NAMES = {
    "PYTHONPATH",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_POSTGRES_PASSWORD",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_JWT_SECRET",
    "AIRFLOW_API_SECRET_KEY",
    MAINTENANCE_DB_CONN_VAR,
}


def child_env() -> dict[str, str]:
    """os.environ minus Airflow internals; component settings come from docker-compose.yml."""
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(_STRIP_PREFIXES) and k not in _STRIP_NAMES
    }
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def run_cli(
    cmd: Sequence[str | Path],
    timeout: timedelta,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    on_line: Callable[[str], None] | None = None,
) -> int:
    """Run a command, stream its output to the task log and return the exit code.

    A watchdog kills the process after `timeout` (keep it below the task's
    execution_timeout); that case is reported as exit code 124 (like coreutils `timeout`).
    `env` defaults to child_env(). `on_line` sees every output line (without newline).
    """
    argv = [str(c) for c in cmd]
    log.info("running: %s", " ".join(argv))
    timed_out = threading.Event()
    with subprocess.Popen(  # noqa: S603 - fixed argv, no shell
        argv,
        cwd=str(cwd) if cwd else None,
        env=dict(env) if env is not None else child_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    ) as proc:

        def _kill() -> None:
            timed_out.set()
            proc.kill()

        watchdog = threading.Timer(timeout.total_seconds(), _kill)
        watchdog.daemon = True
        watchdog.start()
        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                log.info("%s", line.rstrip())
                if on_line is not None:
                    on_line(line.rstrip())
            rc = proc.wait()
        except BaseException:
            # Airflow execution_timeout / task kill: do not leave an orphan behind.
            proc.kill()
            raise
        finally:
            watchdog.cancel()
    if timed_out.is_set():
        log.error("command killed after %s (timeout)", timeout)
        return 124
    log.info("exit code: %s", rc)
    return rc


# --- failure notification ---------------------------------------------------------------
TELEGRAM_API = "https://api.telegram.org"
NOTIFY_TIMEOUT_S = 10


def _send_telegram(text: str) -> bool:
    """Plain HTTPS sendMessage; False if not configured. The token is never logged."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        log.warning("failure notification skipped: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set")
        return False
    body = json.dumps({"chat_id": chat_id, "text": text, "disable_web_page_preview": True})
    req = urllib.request.Request(  # noqa: S310 - fixed https URL
        f"{TELEGRAM_API}/bot{token}/sendMessage",
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=NOTIFY_TIMEOUT_S) as resp:  # noqa: S310
        resp.read()
    return True


def notify_failure(context: Mapping[str, Any]) -> None:
    """DAG-level on_failure_callback: one short Telegram message per failed DAG run.

    Airflow 3 runs DAG-level callbacks in the dag-processor, once the run is FAILED, i.e.
    after the task retries are exhausted (or on dagrun_timeout). The context carries the
    task that caused the failure as `ti`. This function never raises.
    """
    try:
        dag = context.get("dag")
        dag_id = getattr(dag, "dag_id", None) or "?"
        ti = context.get("ti") or context.get("task_instance")
        task_id = getattr(ti, "task_id", None) or "?"
        reason = context.get("reason")
        now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
        suffix = ", zaman aşımı" if reason == "timed_out" else ""
        text = f"⚠️ alacati-hava: {dag_id}.{task_id} başarısız ({now}{suffix})"
        if _send_telegram(text):
            log.info("failure notification sent for %s.%s (reason=%s)", dag_id, task_id, reason)
    except Exception as exc:  # noqa: BLE001 - a callback must never raise
        # Class name only: urllib error text could in theory echo the request URL (token).
        log.warning("failure notification could not be sent: %s", type(exc).__name__)
