"""Open-Meteo Forecast and Marine API client (one HTTP request per point)."""

from __future__ import annotations

import logging
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

log = logging.getLogger(__name__)

# Open-Meteo counts a request as several calls when it asks for more than 10 variables
# or more than 14 days of data.
VARS_PER_CALL = 10
DAYS_PER_CALL = 14


def weighted_calls(n_variables: int, forecast_days: int, past_hours: int) -> float:
    """Calls as Open-Meteo counts them for one request.

    weight = max(1, n_variables / 10) * max(1, (forecast_days + past_hours / 24) / 14)
    """
    variable_weight = max(1.0, n_variables / VARS_PER_CALL)
    day_weight = max(1.0, (forecast_days + past_hours / 24) / DAYS_PER_CALL)
    return variable_weight * day_weight


@dataclass(frozen=True)
class Point:
    """One thing to fetch: a forecast location or a marine zone."""

    source: str  # "forecast" | "marine"
    point_id: str
    latitude: float
    longitude: float


@dataclass(frozen=True)
class ApiConfig:
    """Parameters from config/ingestion.yaml (open_meteo section)."""

    forecast_url: str
    marine_url: str
    timezone: str
    wind_speed_unit: str
    models: str
    expected_units: dict[str, str]
    forecast_days: int
    past_hours: int
    max_catchup_hours: int
    forecast_vars: list[str]
    marine_vars: list[str]
    daily_call_budget: float
    minutely_call_limit: float
    timeout_s: float
    max_retries: int
    backoff_s: float

    @classmethod
    def from_config(cls, ingestion_cfg: dict[str, Any]) -> ApiConfig:
        c = ingestion_cfg["open_meteo"]
        return cls(
            forecast_url=c["forecast_url"],
            marine_url=c["marine_url"],
            timezone=c["timezone"],
            wind_speed_unit=c["wind_speed_unit"],
            models=c["models"],
            expected_units=dict(c["expected_units"]),
            forecast_days=int(c["forecast_days"]),
            past_hours=int(c["past_hours"]),
            max_catchup_hours=int(c["max_catchup_hours"]),
            forecast_vars=list(c["forecast_hourly"]),
            marine_vars=list(c["marine_hourly"]),
            daily_call_budget=float(c["daily_call_budget"]),
            minutely_call_limit=float(c["minutely_call_limit"]),
            timeout_s=float(c["http"]["timeout_s"]),
            max_retries=int(c["http"]["max_retries"]),
            backoff_s=float(c["http"]["backoff_s"]),
        )

    def variables(self, source: str) -> list[str]:
        return self.forecast_vars if source == "forecast" else self.marine_vars


def points_from_config(locations_cfg: dict[str, Any]) -> list[Point]:
    """All forecast locations followed by all marine zones."""
    forecast = [
        Point("forecast", loc["id"], loc["lat"], loc["lon"]) for loc in locations_cfg["locations"]
    ]
    marine = [
        Point("marine", zone["id"], zone["lat"], zone["lon"])
        for zone in locations_cfg["marine_zones"]
    ]
    return forecast + marine


class RateLimiter:
    """Sliding one-minute window over weighted calls."""

    WINDOW_S = 60.0

    def __init__(
        self,
        limit_per_minute: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._limit = limit_per_minute
        self._clock = clock
        self._sleep = sleep
        self._window: deque[tuple[float, float]] = deque()  # (timestamp, weight)

    def acquire(self, weight: float) -> None:
        """Block until `weight` more calls fit into the limit, then record them."""
        while True:
            now = self._clock()
            while self._window and now - self._window[0][0] >= self.WINDOW_S:
                self._window.popleft()
            used = sum(w for _, w in self._window)
            if not self._window or used + weight <= self._limit:
                self._window.append((now, weight))
                return
            self._sleep(self.WINDOW_S - (now - self._window[0][0]))


@dataclass
class CallRecord:
    """One HTTP request; becomes one api_call_log row."""

    call_id: str
    called_at: datetime
    api: str
    point_id: str
    weighted_calls: float
    http_status: int | None
    success: bool
    duration_ms: int | None
    error: str | None
    past_hours: int


@dataclass
class FetchResult:
    """Outcome of fetching one point (after retries)."""

    point: Point
    fetched_at: datetime | None
    payload: dict[str, Any] | None
    calls: list[CallRecord]  # one per HTTP attempt


class UnitsMismatchError(ValueError):
    """hourly_units differ from config expected_units."""


class OpenMeteoClient:
    """Fetches single points, with rate limiting, retries and unit validation."""

    def __init__(
        self,
        config: ApiConfig,
        http: httpx.Client,
        *,
        limiter: RateLimiter | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._cfg = config
        self._http = http
        self._limiter = limiter or RateLimiter(config.minutely_call_limit)
        self._sleep = sleep

    def weight(self, source: str, past_hours: int) -> float:
        n_vars = len(self._cfg.variables(source))
        return weighted_calls(n_vars, self._cfg.forecast_days, past_hours)

    def _params(self, point: Point, past_hours: int) -> dict[str, str | int | float]:
        params: dict[str, str | int | float] = {
            "latitude": point.latitude,
            "longitude": point.longitude,
            "hourly": ",".join(self._cfg.variables(point.source)),
            "timezone": self._cfg.timezone,
            # Open-Meteo ignores forecast_days once past_hours is set and returns the full
            # 16-day range; forecast_hours keeps the window at forecast_days.
            "forecast_hours": self._cfg.forecast_days * 24,
            "past_hours": past_hours,
        }
        if point.source == "forecast":
            params["wind_speed_unit"] = self._cfg.wind_speed_unit
            params["models"] = self._cfg.models
        return params

    def _check_units(self, point: Point, payload: dict[str, Any]) -> None:
        units = payload.get("hourly_units", {})
        for var in self._cfg.variables(point.source):
            expected = self._cfg.expected_units[var]
            if units.get(var) != expected:
                raise UnitsMismatchError(
                    f"unit mismatch for {var}: got {units.get(var)!r}, expected {expected!r}"
                )

    def fetch(self, point: Point, past_hours: int) -> FetchResult:
        """Fetch one point. API problems never raise; a failure has payload None."""
        url = self._cfg.forecast_url if point.source == "forecast" else self._cfg.marine_url
        weight = self.weight(point.source, past_hours)
        calls: list[CallRecord] = []

        for attempt in range(self._cfg.max_retries + 1):
            if attempt:
                delay = self._cfg.backoff_s * 2 ** (attempt - 1)
                log.warning(
                    "retry %d for %s/%s in %.1fs", attempt, point.source, point.point_id, delay
                )
                self._sleep(delay)
            self._limiter.acquire(weight)

            called_at = datetime.now(UTC)
            started = time.monotonic()
            status: int | None = None
            error: str | None = None
            payload: dict[str, Any] | None = None
            retryable = True
            try:
                resp = self._http.get(
                    url, params=self._params(point, past_hours), timeout=self._cfg.timeout_s
                )
                status = resp.status_code
                if status == 200:
                    payload = resp.json()
                    self._check_units(point, payload or {})
                else:
                    error = f"HTTP {status}: {resp.text[:200]}"
                    retryable = status == 429 or status >= 500
            except UnitsMismatchError as exc:
                error, payload, retryable = str(exc), None, False
            except (httpx.HTTPError, ValueError) as exc:
                error, payload = f"{type(exc).__name__}: {exc}", None

            calls.append(
                CallRecord(
                    call_id=str(uuid.uuid4()),
                    called_at=called_at,
                    api=point.source,
                    point_id=point.point_id,
                    weighted_calls=weight,
                    http_status=status,
                    success=payload is not None,
                    duration_ms=int((time.monotonic() - started) * 1000),
                    error=error[:500] if error else None,
                    past_hours=past_hours,
                )
            )
            if payload is not None:
                return FetchResult(point, datetime.now(UTC), payload, calls)
            log.error("call failed for %s/%s: %s", point.source, point.point_id, error)
            if not retryable:
                break
        return FetchResult(point, None, None, calls)
