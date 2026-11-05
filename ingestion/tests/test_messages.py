from __future__ import annotations

import copy
from typing import Any

import pytest
from conftest import NOW, make_payload

from alacati_ingestion.messages import MessageError, build_message, validate_message
from alacati_ingestion.open_meteo import ApiConfig


def _build(config: ApiConfig, source: str, response: dict[str, Any]) -> dict[str, Any]:
    return build_message(
        run_id="11111111-1111-4111-8111-111111111111",
        run_expected_messages=17,
        source=source,
        point_id="dalyan",
        fetched_at=NOW,
        latitude=38.37,
        longitude=26.31,
        forecast_days=config.forecast_days,
        past_hours=3,
        model=config.models,
        variables=config.variables(source),
        response=response,
    )


@pytest.mark.parametrize("source", ["forecast", "marine"])
def test_message_is_contract_valid(config: ApiConfig, source: str) -> None:
    msg = _build(config, source, make_payload(config, source))
    validate_message(msg)
    assert msg["hourly"][0]["time_utc"] == "2026-11-05T00:00:00Z"
    assert "time" not in msg["units"]
    assert msg["grid"] == {"latitude": 38.375, "longitude": 26.3, "elevation_m": 12.0}
    assert msg["model"] == config.models
    assert msg["run_expected_messages"] == 17
    assert msg["fetched_at"] == "2026-11-05T12:30:00Z"


def test_duplicate_hour_rejected(config: ApiConfig) -> None:
    payload = make_payload(config, "forecast")
    payload["hourly"]["time"][1] = payload["hourly"]["time"][0]
    with pytest.raises(MessageError, match="duplicate"):
        _build(config, "forecast", payload)


def test_missing_variable_rejected(config: ApiConfig) -> None:
    payload = make_payload(config, "forecast")
    del payload["hourly"][config.forecast_vars[0]]
    with pytest.raises(MessageError, match="missing variable"):
        _build(config, "forecast", payload)


def test_contract_violation_rejected(config: ApiConfig) -> None:
    payload = make_payload(config, "forecast")
    payload["hourly"]["weather_code"][0] = 150  # contract max is 99
    with pytest.raises(MessageError, match="schema violation"):
        _build(config, "forecast", payload)


def test_extra_property_rejected(config: ApiConfig) -> None:
    bad = copy.deepcopy(_build(config, "marine", make_payload(config, "marine")))
    bad["surprise"] = 1
    with pytest.raises(MessageError):
        validate_message(bad)
