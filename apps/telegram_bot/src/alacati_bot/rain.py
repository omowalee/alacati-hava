"""/yagis: per-region daily rain summary and wet-hour ranges. Pure functions, no I/O.

Open-Meteo convention: ``precipitation`` at hour T is the sum over [T-1h, T). For display a
rainy hour is therefore labelled by its interval START (T-1h), which is what people mean
by "rain at 14:00".
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta

from .formatting import TZ, fmt_hour_label, to_local
from .messages import Row, escape, with_footer
from .settings import BotConfig, RainBin

_HOUR = timedelta(hours=1)


@dataclass(frozen=True)
class RainHour:
    """One location-hour; `start` is the local interval start (T-1h)."""

    region_id: str
    location_id: str
    start: datetime
    mm: float
    prob: float | None
    code: int | None


def rain_window(now_local: datetime) -> tuple[datetime, datetime]:
    """(start, end) of time_utc, both inclusive, covering interval starts from the current
    hour to the end of tomorrow (local).

    start = the next full hour (the current hour's interval starts now); end = local midnight
    after tomorrow (that row's interval starts 23:00 tomorrow).
    """
    hour = now_local.replace(minute=0, second=0, microsecond=0)
    start = (hour + _HOUR).replace(tzinfo=TZ).astimezone(UTC)
    d = now_local.date() + timedelta(days=2)
    return start, datetime(d.year, d.month, d.day, tzinfo=TZ).astimezone(UTC)


def fmt_mm(value: float) -> str:
    """Millimetres with a decimal comma, one decimal."""
    return f"{value:.1f}".replace(".", ",")


def pick_bin(total: float, bins: tuple[RainBin, ...]) -> RainBin:
    """First bin whose exclusive upper bound exceeds total; the last bin has no bound."""
    for b in bins:
        if b.max_mm is None or total < b.max_mm:
            return b
    return bins[-1]


def _prepare(rows: list[Row]) -> list[RainHour]:
    return [
        RainHour(
            region_id=str(r["region_id"]),
            location_id=str(r["location_id"]),
            start=to_local(r["time_utc"]) - _HOUR,
            mm=float(r["precipitation"]),
            prob=None
            if r.get("precipitation_probability") is None
            else float(r["precipitation_probability"]),
            code=None if r.get("weather_code") is None else int(r["weather_code"]),
        )
        for r in rows
        if r.get("precipitation") is not None
    ]


def _day_section(hours: list[RainHour], cfg: BotConfig) -> list[str]:
    """One line per region, sorted by daily total desc (ties: locations.yaml order)."""
    names = cfg.region_names
    order = {rid: i for i, rid in enumerate(names)}
    bins = cfg.params.rain.daily_bins
    loc_total: dict[str, float] = {}
    for h in hours:
        loc_total[h.location_id] = loc_total.get(h.location_id, 0.0) + h.mm
    by_region: dict[str, list[RainHour]] = {}
    for h in hours:
        by_region.setdefault(h.region_id, []).append(h)
    stats = []
    for rid, hs in by_region.items():
        total = max(loc_total[loc] for loc in {h.location_id for h in hs})
        peak = max(hs, key=lambda h: h.mm)
        probs = [h.prob for h in hs if h.prob is not None]
        stats.append((rid, total, peak, max(probs) if probs else None))
    stats.sort(key=lambda s: (-s[1], order.get(s[0], len(order))))
    if all(pick_bin(total, bins) is bins[0] for _, total, _, _ in stats):
        return ["☀️ Yağış beklenmiyor"]
    lines = []
    for rid, total, peak, prob in stats:
        b = pick_bin(total, bins)
        head = f"{b.emoji} {escape(names.get(rid, rid))} · {escape(b.label)}"
        if b is bins[0]:
            lines.append(head)
            continue
        line = f"{head} · {fmt_mm(total)} mm (en yoğun {peak.start:%H:%M}, {fmt_mm(peak.mm)} mm/sa)"
        if prob is not None:
            line += f" · olasılık %{prob:.0f}"
        lines.append(line)
    return lines


def _wet_ranges(hours: list[RainHour], cfg: BotConfig, now_local: datetime) -> list[str]:
    rain, names = cfg.params.rain, cfg.region_names
    first = now_local.replace(minute=0, second=0, microsecond=0)
    last = first + timedelta(hours=rain.span_hours)
    wet = [h for h in hours if h.mm >= rain.wet_hour_mm and first <= h.start < last]
    starts = sorted({h.start for h in wet})
    groups: list[list[datetime]] = []
    for t in starts:
        if groups and t - groups[-1][-1] == _HOUR:
            groups[-1].append(t)
        else:
            groups.append([t])
    today = now_local.date()
    lines = []
    for g in groups:
        members = [h for h in wet if g[0] <= h.start <= g[-1]]
        start, end = g[0], g[-1] + _HOUR
        if end.date() == start.date() or end.time() == time(0):
            span = f"{fmt_hour_label(start, today)}–{end:%H:%M}"
        else:
            span = f"{fmt_hour_label(start, today)} – {fmt_hour_label(end, today)}"
        region_peak: dict[str, float] = {}
        for h in members:
            region_peak[h.region_id] = max(region_peak.get(h.region_id, 0.0), h.mm)
        regions = ", ".join(
            escape(names.get(rid, rid))
            for rid in sorted(region_peak, key=lambda r: -region_peak[r])
        )
        line = f"{span} · en çok {fmt_mm(max(region_peak.values()))} mm/sa · {regions}"
        if any(h.code in rain.thunder_weather_codes for h in members):
            line += " ⛈️ gök gürültülü"
        lines.append(line)
    return lines


def format_rain(rows: list[Row], cfg: BotConfig, now_local: datetime) -> str:
    """/yagis body with the full footer. rows: store.rain_hours for rain_window(now_local)."""
    hours = _prepare(rows)
    today = now_local.date()
    tomorrow = today + timedelta(days=1)
    first = now_local.replace(minute=0, second=0, microsecond=0)
    today_hours = [h for h in hours if h.start.date() == today and h.start >= first]
    tomorrow_hours = [h for h in hours if h.start.date() == tomorrow]
    span = cfg.params.rain.span_hours
    wet = _wet_ranges(hours, cfg, now_local) or [f"Önümüzdeki {span} saatte yağış yok ☀️"]
    parts = [
        "<b>🌧️ Yağış — bugün ve yarın</b>",
        "\n".join(["<b>Bugün</b>", *_day_section(today_hours, cfg)]),
        "\n".join(["<b>Yarın</b>", *_day_section(tomorrow_hours, cfg)]),
        "\n".join([f"<b>Yağışlı saatler (önümüzdeki {span} saat)</b>", *wet]),
    ]
    return with_footer("\n\n".join(parts), cfg)
