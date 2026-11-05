"""Pure formatting helpers (no Streamlit / BigQuery) so they can be unit tested."""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Istanbul")
KMH_PER_KNOT = 1.852

# Presentation only: level colours and Turkish names.
LEVEL_COLORS = {"yellow": "#f2c200", "orange": "#f57c00", "red": "#d32f2f"}
LEVEL_TEXT_COLORS = {"yellow": "#000000", "orange": "#ffffff", "red": "#ffffff"}
LEVEL_NAMES_TR = {"yellow": "Sarı", "orange": "Turuncu", "red": "Kırmızı"}
SPORT_NAMES_TR = {"windsurf": "Windsurf", "kitesurf": "Kitesurf"}
QUALITY_NAMES_TR = {"ideal": "İdeal", "rideable": "Yapılabilir"}

_COMPASS_TR = ["K", "KD", "D", "GD", "G", "GB", "B", "KB"]
_ARROWS = ["↑", "↗", "→", "↘", "↓", "↙", "←", "↖"]
_WEEKDAYS_TR = ["Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"]


def _is_missing(value: float | None) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def to_local(ts: datetime) -> datetime:
    """Convert a UTC (or naive-UTC) datetime to naive Europe/Istanbul time."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(TZ).replace(tzinfo=None)


def kmh_to_knots(kmh: float) -> float:
    return kmh / KMH_PER_KNOT


def fmt_time(ts: datetime) -> str:
    return f"{ts:%H:%M}"


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
        return f"{fmt_day_time(start, today)} – {fmt_time(end)}"
    return f"{fmt_day_time(start, today)} – {fmt_day_time(end, today)}"


def fmt_number(value: float, unit: str) -> str:
    """Value with a sensible number of decimals and a decimal comma."""
    text = f"{value:.0f}" if abs(value) >= 10 else f"{value:.1f}"
    return f"{text.replace('.', ',')} {unit}"


def compass(deg: float | None) -> str:
    """Turkish compass text for a wind direction, e.g. 'KD (45°)'."""
    if _is_missing(deg):
        return "–"
    assert deg is not None
    idx = int(((deg % 360) + 22.5) // 45) % 8
    return f"{_COMPASS_TR[idx]} ({deg:.0f}°)"


def wind_arrow(deg_from: float | None) -> str:
    """Arrow pointing where the wind blows TO (direction is meteorological FROM)."""
    if _is_missing(deg_from):
        return ""
    assert deg_from is not None
    idx = int((((deg_from + 180) % 360) + 22.5) // 45) % 8
    return _ARROWS[idx]


def score_color(score: float | None) -> list[int]:
    """RGB for a 0-100 score, red (0) -> yellow (50) -> green (100)."""
    if _is_missing(score):
        return [150, 150, 150]
    assert score is not None
    s = max(0.0, min(100.0, float(score))) / 100.0
    if s < 0.5:
        r, g = 214, int(40 + (s / 0.5) * 170)
    else:
        r, g = int(214 - ((s - 0.5) / 0.5) * 170), 210
    return [r, g, 60]


def level_badge_html(level: str) -> str:
    """Coloured pill with the Turkish level name."""
    bg = LEVEL_COLORS.get(level, "#888888")
    fg = LEVEL_TEXT_COLORS.get(level, "#ffffff")
    name = LEVEL_NAMES_TR.get(level, level)
    return (
        f"<span style='background:{bg};color:{fg};padding:2px 10px;border-radius:12px;"
        f"font-weight:600;font-size:0.85em'>{name}</span>"
    )


def region_list(ids: Sequence[str], names: dict[str, str]) -> str:
    """Comma-separated region names (falls back to the id)."""
    return ", ".join(names.get(i, i) for i in ids)
