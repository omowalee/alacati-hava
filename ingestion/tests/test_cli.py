from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from alacati_ingestion import cli
from alacati_ingestion.runner import RunResult, RunSummary

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _result(status: str, ops_failed: bool = False) -> RunResult:
    summary = RunSummary("r", "manual", NOW, 3, 1, NOW, 1, 0, status)
    return RunResult(summary, ops_failed=ops_failed)


@pytest.mark.parametrize(
    ("status", "ops_failed", "code"),
    [
        ("complete", False, 0),
        ("partial", False, 2),
        ("failed", False, 1),
        ("complete", True, 1),
        ("partial", True, 1),
    ],
)
def test_exit_code(status: str, ops_failed: bool, code: int) -> None:
    assert cli.exit_code(_result(status, ops_failed)) == code


def test_cmd_run_dry_run_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli.ApiConfig, "from_config", lambda _c: object())
    monkeypatch.setattr(cli, "load_config", lambda _n: {})
    monkeypatch.setattr(cli, "points_from_config", lambda _c: [])
    monkeypatch.setattr(cli, "OpenMeteoClient", lambda *_a, **_k: object())

    def fake_execute(**_kw: Any) -> RunResult:
        return _result("partial")

    monkeypatch.setattr(cli, "execute_run", fake_execute)
    assert cli.cmd_run("manual", True) == 2
