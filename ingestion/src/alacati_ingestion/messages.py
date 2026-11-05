"""Build and validate weather.raw v1 messages (contracts/kafka/weather_raw.v1.schema.json).

This module knows nothing about BigQuery, so the Phase 3 Kafka producer can reuse it.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from functools import cache
from typing import Any

from jsonschema import Draft202012Validator

from alacati_ingestion.settings import CONTRACTS_DIR

SCHEMA_VERSION = 1
SCHEMA_PATH = CONTRACTS_DIR / "kafka" / "weather_raw.v1.schema.json"


class MessageError(ValueError):
    """The API response cannot be turned into a valid weather.raw message."""


@cache
def _validator() -> Draft202012Validator:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


def to_utc_iso(open_meteo_time: str) -> str:
    """Convert an Open-Meteo GMT time such as 2026-11-05T00:00 to 2026-11-05T00:00:00Z.

    The 'Z' marks UTC; seconds are added because RFC 3339 date-time requires them.
    """
    try:
        parsed = datetime.fromisoformat(open_meteo_time)
    except ValueError as exc:
        raise MessageError(f"bad time value {open_meteo_time!r}") from exc
    return parsed.replace(tzinfo=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_hours(response: dict[str, Any], variables: list[str]) -> list[dict[str, Any]]:
    hourly = response.get("hourly")
    if not isinstance(hourly, dict) or "time" not in hourly:
        raise MessageError("response has no hourly.time")
    times: list[str] = hourly["time"]
    columns: dict[str, list[Any]] = {}
    for var in variables:
        if var not in hourly:
            raise MessageError(f"response is missing variable {var!r}")
        if len(hourly[var]) != len(times):
            raise MessageError(f"variable {var!r} length differs from time length")
        columns[var] = hourly[var]

    hours = [
        {"time_utc": to_utc_iso(t), **{var: columns[var][i] for var in variables}}
        for i, t in enumerate(times)
    ]
    seen: set[str] = set()
    for hour in hours:
        if hour["time_utc"] in seen:
            raise MessageError(f"duplicate time_utc {hour['time_utc']}")
        seen.add(hour["time_utc"])
    return hours


def validate_message(message: dict[str, Any]) -> None:
    """Raise MessageError if the message violates the contract."""
    errors = sorted(_validator().iter_errors(message), key=lambda e: list(e.absolute_path))
    if errors:
        first = errors[0]
        path = "/".join(str(p) for p in first.absolute_path) or "<root>"
        raise MessageError(f"schema violation at {path}: {first.message}")


def build_message(
    *,
    run_id: str,
    run_expected_messages: int,
    source: str,
    point_id: str,
    fetched_at: datetime,
    latitude: float,
    longitude: float,
    forecast_days: int,
    past_hours: int,
    model: str,
    variables: list[str],
    response: dict[str, Any],
) -> dict[str, Any]:
    """Turn one Open-Meteo response into a validated weather.raw v1 message."""
    units = {k: v for k, v in response.get("hourly_units", {}).items() if k != "time"}
    message: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "message_id": str(uuid.uuid4()),
        "run_id": run_id,
        "run_expected_messages": run_expected_messages,
        "source": source,
        "point_id": point_id,
        "fetched_at": fetched_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "request": {
            "latitude": latitude,
            "longitude": longitude,
            "forecast_days": forecast_days,
            "past_hours": past_hours,
        },
        "grid": {
            "latitude": response.get("latitude"),
            "longitude": response.get("longitude"),
            "elevation_m": response.get("elevation"),
        },
        "model": model,
        "units": units,
        "hourly": _build_hours(response, variables),
    }
    validate_message(message)
    return message
