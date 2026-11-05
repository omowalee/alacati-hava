"""Shared fixtures: config from config/, fake API payloads, fake run store."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from alacati_ingestion.open_meteo import ApiConfig, CallRecord, Point, points_from_config
from alacati_ingestion.runner import RunSummary
from alacati_ingestion.settings import load_config

NOW = datetime(2026, 11, 5, 12, 30, tzinfo=UTC)


@pytest.fixture
def config() -> ApiConfig:
    return ApiConfig.from_config(load_config("ingestion"))


@pytest.fixture
def points() -> list[Point]:
    return points_from_config(load_config("locations"))


def make_payload(config: ApiConfig, source: str, hours: int = 4) -> dict[str, Any]:
    """A realistic Open-Meteo response for the configured variables."""
    variables = config.variables(source)
    times = [f"2026-11-05T{h:02d}:00" for h in range(hours)]
    return {
        "latitude": 38.375,
        "longitude": 26.3,
        "elevation": 12.0,
        "hourly_units": {"time": "iso8601", **{v: config.expected_units[v] for v in variables}},
        "hourly": {"time": times, **{v: [1.0] * hours for v in variables}},
    }


class FakeStore:
    """In-memory RunStore."""

    def __init__(
        self,
        used_today: float = 0.0,
        last_complete: datetime | None = None,
        fail_sources: tuple[str, ...] = (),
        fail_ops: bool = False,
    ) -> None:
        self.used_today = used_today
        self.last_complete = last_complete
        self.fail_sources = fail_sources
        self.fail_ops = fail_ops
        self.raw_loaded: list[dict[str, Any]] = []
        self.ops: tuple[list[CallRecord], RunSummary] | None = None

    def todays_weighted_calls(self, now: datetime) -> float:
        return self.used_today

    def last_complete_run_started_at(self) -> datetime | None:
        return self.last_complete

    def load_raw(self, run_id: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        self.raw_loaded = [m for m in messages if m["source"] not in self.fail_sources]
        return self.raw_loaded

    def load_ops(self, calls: list[CallRecord], summary: RunSummary) -> None:
        self.ops = (calls, summary)
        if self.fail_ops:
            raise RuntimeError("ops boom")
