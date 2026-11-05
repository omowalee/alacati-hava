"""Pure formatting helpers (time, wind, levels)."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Istanbul")
KMH_PER_KNOT = 1.852
LEVEL_EMOJI = {"yellow": "🟡", "orange": "🟠", "red": "🔴"}
SPORT_TR = {"windsurf": "Windsurf", "kitesurf": "Kitesurf"}
SURF_SPORT_TR = {"windsurf": "Rüzgâr sörfü", "kitesurf": "Kite"}
SURF_QUALITY_TR = {"ideal": "✅ ideal", "rideable": "👍 sörf yapılabilir"}
QUALITY_TR = {"ideal": "ideal", "rideable": "yapılabilir"}

_COMPASS_TR = ["K", "KD", "D", "GD", "G", "GB", "B", "KB"]
_ARROWS = ["↑", "↗", "→", "↘", "↓", "↙", "←", "↖"]
_WEEKDAYS_TR = ["Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"]


def to_local(ts: datetime) -> datetime:
    """UTC (or naive-UTC) datetime to naive Europe/Istanbul time."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(TZ).replace(tzinfo=None)


def fmt_day_time(ts: datetime, today: date) -> str:
    """'Bugün 14:00', 'Yarın 09:00' or 'Per 06.11. 09:00'."""
    delta = (ts.date() - today).days
    if delta == 0:
        day = "Bugün"
    elif delta == 1:
        day = "Yarın"
    else:
        day = f"{_WEEKDAYS_TR[ts.weekday()]} {ts:%d.%m.}"
    return f"{day} {ts:%H:%M}"


def fmt_range(start: datetime, end: datetime, today: date) -> str:
    """Local start-end range; the end repeats the day only if it differs."""
    if start.date() == end.date():
        return f"{fmt_day_time(start, today)}–{end:%H:%M}"
    return f"{fmt_day_time(start, today)} – {fmt_day_time(end, today)}"


def fmt_number(value: float, unit: str) -> str:
    """Value with a decimal comma."""
    text = f"{value:.0f}" if abs(value) >= 10 else f"{value:.1f}"
    return f"{text.replace('.', ',')} {unit}"


def kmh_to_knots(kmh: float) -> float:
    return kmh / KMH_PER_KNOT


def compass(deg: float | None) -> str:
    """Turkish compass text, e.g. 'KD'."""
    if deg is None or math.isnan(deg):
        return "–"
    return _COMPASS_TR[int(((deg % 360) + 22.5) // 45) % 8]


def wind_arrow(deg_from: float | None) -> str:
    """Arrow pointing where the wind blows TO (input is meteorological FROM)."""
    if deg_from is None or math.isnan(deg_from):
        return ""
    return _ARROWS[int((((deg_from + 180) % 360) + 22.5) // 45) % 8]
