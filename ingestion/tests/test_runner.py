from __future__ import annotations

from datetime import timedelta

import httpx
import respx
from conftest import NOW, FakeStore, make_payload

from alacati_ingestion.open_meteo import ApiConfig, OpenMeteoClient, Point, RateLimiter
from alacati_ingestion.runner import RunResult, compute_past_hours, execute_run, final_status


def _run(
    config: ApiConfig, points: list[Point], store: FakeStore | None, trigger: str = "schedule"
) -> RunResult:
    with httpx.Client() as http:
        client = OpenMeteoClient(config, http, limiter=RateLimiter(10_000), sleep=lambda _s: None)
        return execute_run(
            config=config,
            points=points,
            client=client,
            store=store,
            cli_trigger=trigger,
            now=lambda: NOW,
        )


def _mock_apis(config: ApiConfig, marine_status: int = 200) -> None:
    respx.get(config.forecast_url).respond(json=make_payload(config, "forecast"))
    if marine_status == 200:
        respx.get(config.marine_url).respond(json=make_payload(config, "marine"))
    else:
        respx.get(config.marine_url).respond(marine_status)


def test_past_hours_defaults_to_config(config: ApiConfig) -> None:
    assert compute_past_hours(config, None, NOW) == config.past_hours
    recent = NOW - timedelta(minutes=50)
    assert compute_past_hours(config, recent, NOW) == config.past_hours


def test_past_hours_catchup_rounds_up(config: ApiConfig) -> None:
    last = NOW - timedelta(hours=10, minutes=1)
    assert compute_past_hours(config, last, NOW) == 11


def test_past_hours_capped(config: ApiConfig) -> None:
    last = NOW - timedelta(days=30)
    assert compute_past_hours(config, last, NOW) == config.max_catchup_hours


def test_final_status() -> None:
    assert final_status(17, 17) == "complete"
    assert final_status(17, 5) == "partial"
    assert final_status(17, 0) == "failed"


@respx.mock
def test_complete_run(config: ApiConfig, points: list[Point]) -> None:
    _mock_apis(config)
    store = FakeStore()
    s = _run(config, points, store).summary
    assert (s.status, s.succeeded_messages, s.failed_messages) == ("complete", len(points), 0)
    assert s.trigger == "schedule"
    assert s.past_hours == config.past_hours
    assert store.ops is not None
    assert len(store.ops[0]) == len(points)
    assert {m["run_id"] for m in store.raw_loaded} == {s.run_id}
    assert {m["run_expected_messages"] for m in store.raw_loaded} == {len(points)}


@respx.mock
def test_partial_run_when_marine_fails(config: ApiConfig, points: list[Point]) -> None:
    _mock_apis(config, marine_status=400)
    result = _run(config, points, FakeStore())
    assert result.summary.status == "partial"
    assert result.summary.failed_messages == sum(p.source == "marine" for p in points)


@respx.mock
def test_failed_run_when_everything_fails(config: ApiConfig, points: list[Point]) -> None:
    respx.get(config.forecast_url).respond(400)
    respx.get(config.marine_url).respond(400)
    assert _run(config, points, FakeStore()).summary.status == "failed"


@respx.mock
def test_load_failure_counts_as_failed(config: ApiConfig, points: list[Point]) -> None:
    _mock_apis(config)
    result = _run(config, points, FakeStore(fail_sources=("marine",)))
    assert result.summary.status == "partial"


@respx.mock
def test_catchup_trigger(config: ApiConfig, points: list[Point]) -> None:
    _mock_apis(config)
    store = FakeStore(last_complete=NOW - timedelta(hours=20))
    result = _run(config, points, store, trigger="schedule")
    assert result.summary.trigger == "catchup"
    assert result.summary.past_hours == 20


@respx.mock
def test_budget_abort_makes_no_requests(config: ApiConfig, points: list[Point]) -> None:
    forecast = respx.get(config.forecast_url).respond(json=make_payload(config, "forecast"))
    store = FakeStore(used_today=config.daily_call_budget - 1)
    result = _run(config, points, store)
    assert result.summary.status == "failed"
    assert "budget" in result.note
    assert forecast.call_count == 0
    assert store.ops is not None
    assert store.ops[1].status == "failed"


@respx.mock
def test_dry_run_skips_budget_and_store(config: ApiConfig, points: list[Point]) -> None:
    _mock_apis(config)
    result = _run(config, points, None)
    assert result.summary.status == "complete"
    assert len(result.messages) == len(points)
