from __future__ import annotations

import httpx
import pytest
import respx
from conftest import make_payload

from alacati_ingestion.open_meteo import (
    ApiConfig,
    OpenMeteoClient,
    Point,
    RateLimiter,
    weighted_calls,
)

POINT = Point("forecast", "dalyan", 38.37, 26.31)
MARINE = Point("marine", "north", 38.375, 26.29)


def _client(config: ApiConfig, http: httpx.Client) -> OpenMeteoClient:
    return OpenMeteoClient(config, http, limiter=RateLimiter(10_000), sleep=lambda _s: None)


def test_weighted_calls_formula() -> None:
    assert weighted_calls(8, 3, 3) == 1.0
    assert weighted_calls(20, 3, 0) == 2.0  # 20 variables
    assert weighted_calls(8, 14, 0) == 1.0
    assert weighted_calls(8, 14, 14 * 24) == 2.0  # 28 days
    assert weighted_calls(30, 28, 0) == 6.0


@respx.mock
def test_forecast_request_params_and_success(config: ApiConfig) -> None:
    route = respx.get(config.forecast_url).respond(json=make_payload(config, "forecast"))
    with httpx.Client() as http:
        result = _client(config, http).fetch(POINT, past_hours=5)
    assert result.payload is not None
    params = route.calls[0].request.url.params
    assert params["timezone"] == "GMT"
    assert params["past_hours"] == "5"
    # forecast_days is ignored by Open-Meteo when past_hours is set -> must send forecast_hours
    assert "forecast_days" not in params
    assert params["forecast_hours"] == str(config.forecast_days * 24)
    assert params["wind_speed_unit"] == config.wind_speed_unit
    assert params["hourly"].split(",") == config.forecast_vars
    assert len(result.calls) == 1
    assert result.calls[0].success


@respx.mock
def test_marine_uses_marine_url(config: ApiConfig) -> None:
    respx.get(config.marine_url).respond(json=make_payload(config, "marine"))
    with httpx.Client() as http:
        result = _client(config, http).fetch(MARINE, past_hours=3)
    assert result.payload is not None
    assert result.calls[0].api == "marine"


@respx.mock
def test_units_mismatch_is_failed_call_without_retry(config: ApiConfig) -> None:
    payload = make_payload(config, "forecast")
    payload["hourly_units"]["wind_speed_10m"] = "m/s"
    route = respx.get(config.forecast_url).respond(json=payload)
    with httpx.Client() as http:
        result = _client(config, http).fetch(POINT, past_hours=3)
    assert result.payload is None
    assert route.call_count == 1
    assert not result.calls[0].success
    assert "unit mismatch" in (result.calls[0].error or "")


@respx.mock
def test_retries_server_errors_then_succeeds(config: ApiConfig) -> None:
    route = respx.get(config.forecast_url).mock(
        side_effect=[
            httpx.Response(503),
            httpx.ConnectError("boom"),
            httpx.Response(200, json=make_payload(config, "forecast")),
        ]
    )
    with httpx.Client() as http:
        result = _client(config, http).fetch(POINT, past_hours=3)
    assert result.payload is not None
    assert route.call_count == 3
    assert [c.success for c in result.calls] == [False, False, True]
    assert result.calls[0].http_status == 503
    assert result.calls[1].http_status is None


@respx.mock
def test_client_error_is_not_retried(config: ApiConfig) -> None:
    route = respx.get(config.forecast_url).respond(400, json={"reason": "bad"})
    with httpx.Client() as http:
        result = _client(config, http).fetch(POINT, past_hours=3)
    assert result.payload is None
    assert route.call_count == 1


@respx.mock
def test_gives_up_after_max_retries(config: ApiConfig) -> None:
    route = respx.get(config.forecast_url).respond(500)
    with httpx.Client() as http:
        result = _client(config, http).fetch(POINT, past_hours=3)
    assert result.payload is None
    assert route.call_count == config.max_retries + 1


def test_rate_limiter_waits_when_window_full() -> None:
    now = [0.0]
    slept: list[float] = []

    def sleep(s: float) -> None:
        slept.append(s)
        now[0] += s

    limiter = RateLimiter(2, clock=lambda: now[0], sleep=sleep)
    limiter.acquire(1)
    limiter.acquire(1)
    assert slept == []
    limiter.acquire(1)  # window full: must wait for the first call to leave the minute
    assert slept == [pytest.approx(60.0)]
