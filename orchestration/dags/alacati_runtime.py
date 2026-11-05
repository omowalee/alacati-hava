"""Shared helpers for the alacati-hava DAGs (not a DAG file; listed in .airflowignore).

Every component (ingestion, dbt, bot) runs as a subprocess from its own isolated uv venv
under /opt/venvs, never inside Airflow's Python environment. Output is streamed line by
line into the Airflow task log.
"""

from __future__ import annotations

import logging
import os
import subprocess
import threading
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

log = logging.getLogger(__name__)

# Locations inside the custom Airflow image (docker/airflow/Dockerfile).
APP_ROOT = Path(os.environ.get("ALACATI_APP_ROOT", "/opt/alacati"))
VENVS = Path(os.environ.get("ALACATI_VENVS", "/opt/venvs"))

INGEST_BIN = VENVS / "ingestion" / "bin" / "alacati-ingest"
DBT_PYTHON = VENVS / "dbt" / "bin" / "python"
RUN_DBT = APP_ROOT / "dbt" / "run_dbt.py"
SUMMARY_BIN = VENVS / "bot" / "bin" / "alacati-bot-summary"

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
}


def child_env() -> dict[str, str]:
    """os.environ minus Airflow internals; component settings come from .env (env_file)."""
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(_STRIP_PREFIXES) and k not in _STRIP_NAMES
    }
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def run_cli(cmd: Sequence[str | Path], timeout: timedelta, cwd: Path | None = None) -> int:
    """Run a command, stream its output to the task log and return the exit code.

    A watchdog kills the process after `timeout` (keep it below the task's
    execution_timeout); that case is reported as exit code 124 (like coreutils `timeout`).
    """
    argv = [str(c) for c in cmd]
    log.info("running: %s", " ".join(argv))
    timed_out = threading.Event()
    with subprocess.Popen(  # noqa: S603 - fixed argv, no shell
        argv,
        cwd=str(cwd) if cwd else None,
        env=child_env(),
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
