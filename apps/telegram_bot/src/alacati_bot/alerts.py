"""Validate weather.alerts v1 notifications and render them as Turkish Telegram HTML."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from functools import cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from .formatting import LEVEL_EMOJI, fmt_number, fmt_range, to_local
from .messages import escape, truncate_message, with_footer
from .settings import BotConfig, contracts_dir

SCHEMA_NAME = "weather_alert.v1.schema.json"

STATUS_HEADER = {
    "new": "🆕 Yeni uyarı",
    "updated": "🔄 Uyarı güncellendi",
    "ended": "✅ Uyarı sona erdi",
}
LEVEL_TR = {"yellow": "Sarı", "orange": "Turuncu", "red": "Kırmızı"}


class AlertError(ValueError):
    """The Kafka payload is not a valid weather.alerts v1 message."""


@cache
def _validator(contracts: Path) -> Draft202012Validator:
    schema = json.loads((contracts / "kafka" / SCHEMA_NAME).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


def parse_alert(payload: bytes, contracts: Path | None = None) -> dict[str, Any]:
    """Decode and validate one Kafka value; raise AlertError if it violates the contract."""
    try:
        data = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise AlertError(f"not valid UTF-8 JSON: {exc}") from exc
    errors = sorted(
        _validator(contracts or contracts_dir()).iter_errors(data),
        key=lambda e: list(e.absolute_path),
    )
    if errors:
        path = "/".join(str(p) for p in errors[0].absolute_path) or "<root>"
        raise AlertError(f"schema violation at {path}: {errors[0].message}")
    assert isinstance(data, dict)
    return data


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def is_stale(alert: dict[str, Any], now_utc: datetime, max_age_minutes: float) -> bool:
    """True if the notification was issued longer than max_age_minutes ago."""
    age = now_utc - parse_utc(str(alert["issued_at"]))
    return age.total_seconds() > max_age_minutes * 60


def _level(level: str) -> str:
    return LEVEL_TR[level].lower()


def _reasons(alert: dict[str, Any], today: date) -> list[str]:
    lines: list[str] = []
    for reason in alert["change_reasons"]:
        match reason:
            case "level_up" | "level_down":
                word = "yükseldi" if reason == "level_up" else "düştü"
                lines.append(
                    f"seviye {word}: {_level(alert['previous_level'])} → {_level(alert['level'])}"
                )
            case "window_shift":
                prev_range = fmt_range(
                    to_local(parse_utc(alert["previous_start_utc"])),
                    to_local(parse_utc(alert["previous_end_utc"])),
                    today,
                )
                lines.append(f"zaman aralığı değişti: önceki {prev_range}")
            case "region_added":
                lines.append("yeni bölge eklendi")
            case "forecast_cleared":
                lines.append("güncel tahminde artık görünmüyor")
            case "window_passed":
                lines.append("uyarı süresi sona erdi")
    return lines


def _regions(alert: dict[str, Any], cfg: BotConfig) -> str:
    names = cfg.region_names
    levels = {r["region_id"]: r["level"] for r in alert["region_levels"]}
    differ = len(set(levels.values())) > 1
    parts = []
    for region in alert["affected_regions"]:
        name = escape(names.get(region, region))
        if differ and region in levels:
            name += f" ({LEVEL_TR[levels[region]]})"
        parts.append(name)
    return ", ".join(parts)


def format_alert(alert: dict[str, Any], cfg: BotConfig) -> str:
    """Turkish HTML message for one notification, with disclaimer and attribution footer."""
    today = to_local(parse_utc(str(alert["issued_at"]))).date()
    label = cfg.hazard_labels.get(alert["hazard"], alert["hazard"])
    level = alert["level"]
    window = fmt_range(
        to_local(parse_utc(alert["start_utc"])), to_local(parse_utc(alert["end_utc"])), today
    )
    peak_local = to_local(parse_utc(alert["peak_time_utc"]))
    peak = escape(fmt_number(float(alert["peak_value"]), str(alert["peak_unit"])))
    lines = [
        f"<b>{STATUS_HEADER[alert['status']]}</b>",
        f"{LEVEL_EMOJI[level]} <b>{LEVEL_TR[level]} seviye</b> · {escape(label)}",
        f"🕐 {window}",
        f"📈 Zirve: {peak} ({peak_local:%H:%M})",
        f"📍 {_regions(alert, cfg)}",
    ]
    if alert["status"] != "new":
        reasons = _reasons(alert, today)
        if reasons:
            lines.append("")
            lines.extend(f"• {r}" for r in reasons)
    return truncate_message(with_footer("\n".join(lines), cfg), cfg)
