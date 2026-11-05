"""One ingestion run: decide past_hours, check the budget, fetch, validate, store."""

from __future__ import annotations

import logging
import math
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from alacati_ingestion.messages import MessageError, build_message
from alacati_ingestion.open_meteo import (
    ApiConfig,
    CallRecord,
    OpenMeteoClient,
    Point,
)

log = logging.getLogger(__name__)


class RunStore(Protocol):
    """Where run state lives. Implemented by bq_loader.BigQueryStore (and by test fakes)."""

    def todays_weighted_calls(self, now: datetime) -> float: ...

    def last_complete_run_started_at(self) -> datetime | None: ...

    def load_raw(self, run_id: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Load messages; return the ones that were stored successfully."""
        ...

    def load_ops(self, calls: list[CallRecord], summary: RunSummary) -> None: ...


@dataclass
class RunSummary:
    """Becomes one alacati_ops.ingest_runs row."""

    run_id: str
    trigger: str  # schedule | catchup | manual
    started_at: datetime
    past_hours: int
    expected_messages: int
    finished_at: datetime | None = None
    succeeded_messages: int = 0
    failed_messages: int = 0
    status: str = "running"  # running | complete | partial | failed


@dataclass
class RunResult:
    summary: RunSummary
    messages: list[dict[str, Any]] = field(default_factory=list)
    calls: list[CallRecord] = field(default_factory=list)
    note: str = ""  # e.g. why the run was aborted


def compute_past_hours(
    config: ApiConfig, last_complete_started_at: datetime | None, now: datetime
) -> int:
    """past_hours = max(config value, hours since the last complete run, rounded up), capped."""
    wanted = config.past_hours
    if last_complete_started_at is not None:
        hours_since = math.ceil((now - last_complete_started_at).total_seconds() / 3600)
        wanted = max(wanted, hours_since)
    return min(wanted, config.max_catchup_hours)


def final_status(expected: int, succeeded: int) -> str:
    if succeeded >= expected:
        return "complete"
    return "partial" if succeeded > 0 else "failed"


def execute_run(
    *,
    config: ApiConfig,
    points: list[Point],
    client: OpenMeteoClient,
    store: RunStore | None,
    cli_trigger: str,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> RunResult:
    """Run one ingestion. With store=None (dry run) nothing is read from or written to BigQuery."""
    started_at = now()
    expected = len(points)

    last_started = store.last_complete_run_started_at() if store else None
    past_hours = compute_past_hours(config, last_started, started_at)
    trigger = "catchup" if past_hours > config.past_hours else cli_trigger
    summary = RunSummary(str(uuid.uuid4()), trigger, started_at, past_hours, expected)
    result = RunResult(summary)
    log.info(
        "run %s: trigger=%s past_hours=%d points=%d", summary.run_id, trigger, past_hours, expected
    )

    estimated = sum(client.weight(p.source, past_hours) for p in points)
    if store is not None:
        used_today = store.todays_weighted_calls(started_at)
        log.info(
            "budget: used today %.1f + this run %.1f vs limit %.0f",
            used_today,
            estimated,
            config.daily_call_budget,
        )
        if used_today + estimated > config.daily_call_budget:
            result.note = (
                f"daily call budget exceeded ({used_today:.1f} used + {estimated:.1f} "
                f"estimated > {config.daily_call_budget:.0f}); run aborted"
            )
            log.error(result.note)
            summary.failed_messages = expected
            summary.status = "failed"
            summary.finished_at = now()
            store.load_ops([], summary)
            return result
    else:
        log.info("dry run: budget check skipped (estimated %.1f weighted calls)", estimated)

    for point in points:
        fetched = client.fetch(point, past_hours)
        result.calls.extend(fetched.calls)
        if fetched.payload is None or fetched.fetched_at is None:
            continue
        try:
            result.messages.append(
                build_message(
                    run_id=summary.run_id,
                    run_expected_messages=expected,
                    source=point.source,
                    point_id=point.point_id,
                    fetched_at=fetched.fetched_at,
                    latitude=point.latitude,
                    longitude=point.longitude,
                    forecast_days=config.forecast_days,
                    past_hours=past_hours,
                    model=config.models,
                    variables=config.variables(point.source),
                    response=fetched.payload,
                )
            )
        except MessageError as exc:
            log.error("invalid message for %s/%s: %s", point.source, point.point_id, exc)
            last_call = fetched.calls[-1]
            last_call.success = False
            last_call.error = f"invalid message: {exc}"[:500]

    log.info(
        "fetched %d/%d points; %d HTTP requests, %.1f weighted calls",
        len(result.messages),
        expected,
        len(result.calls),
        sum(c.weighted_calls for c in result.calls),
    )

    if store is not None:
        result.messages = store.load_raw(summary.run_id, result.messages)
    summary.succeeded_messages = len(result.messages)
    summary.failed_messages = expected - summary.succeeded_messages
    summary.status = final_status(expected, summary.succeeded_messages)
    summary.finished_at = now()
    if store is not None:
        store.load_ops(result.calls, summary)
    log.info("run %s finished: %s", summary.run_id, summary.status)
    return result
