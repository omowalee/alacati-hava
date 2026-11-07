"""21:30 night summary: rain, wind and alerts from the next full hour until the morning.

Pure functions, no I/O. Rain uses the Open-Meteo convention of rain.py (the value at T
covers [T-1h, T), labelled by its interval start); wind is the instantaneous value at T.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .formatting import TZ, compass, fmt_day_time, kmh_to_knots, to_local, weekday_tr, wind_arrow
from .messages import Row, escape, format_alerts_section, no_alerts_text, with_footer
from .rain import RainHour, prepare_hours, region_lines, wet_ranges
from .settings import BotConfig, WindBin

_HOUR = timedelta(hours=1)


@dataclass(frozen=True)
class NightWind:
    """Peninsula-wide wind of the night (strongest point and hour)."""

    max_kmh: float
    max_gust_kmh: float | None
    direction_deg: float | None  # speed-weighted mean direction (FROM), all points and hours
    peak_region: str
    peak_time: datetime  # local


def night_window(now_local: datetime, cfg: BotConfig) -> tuple[datetime, datetime]:
    """(start, end) in local time: the next full hour until end_local_hour the next morning
    (or this morning, if it is already past midnight)."""
    end_hour = cfg.params.night.end_local_hour
    start = now_local.replace(minute=0, second=0, microsecond=0) + _HOUR
    end_day = now_local.date() + timedelta(days=0 if now_local.hour < end_hour else 1)
    return start, datetime(end_day.year, end_day.month, end_day.day, end_hour)


def query_range(start_local: datetime, end_local: datetime) -> tuple[datetime, datetime]:
    """time_utc range (inclusive) for store.rain_hours: wind at start..end, and rain
    intervals starting start..end-1h (stored at start+1h..end)."""
    return (
        start_local.replace(tzinfo=TZ).astimezone(UTC),
        end_local.replace(tzinfo=TZ).astimezone(UTC),
    )


def pick_wind_bin(kmh: float, bins: tuple[WindBin, ...]) -> WindBin:
    for b in bins:
        if b.max_kmh is None or kmh < b.max_kmh:
            return b
    return bins[-1]


def _mean_direction(rows: list[Row]) -> float | None:
    """Speed-weighted circular mean of wind_direction_10m (meteorological FROM)."""
    x = y = 0.0
    for r in rows:
        d, v = r.get("wind_direction_10m"), r.get("wind_speed_10m")
        if d is None or v is None:
            continue
        x += float(v) * math.sin(math.radians(float(d)))
        y += float(v) * math.cos(math.radians(float(d)))
    if x == 0.0 and y == 0.0:
        return None
    return math.degrees(math.atan2(x, y)) % 360


def night_wind(rows: list[Row], start: datetime, end: datetime) -> NightWind | None:
    """Wind rows with start <= local time <= end; None if there is no wind data."""
    hours = [
        r
        for r in rows
        if r.get("wind_speed_10m") is not None and start <= to_local(r["time_utc"]) <= end
    ]
    if not hours:
        return None
    peak = max(hours, key=lambda r: float(r["wind_speed_10m"]))
    gusts = [float(r["wind_gusts_10m"]) for r in hours if r.get("wind_gusts_10m") is not None]
    return NightWind(
        max_kmh=float(peak["wind_speed_10m"]),
        max_gust_kmh=max(gusts) if gusts else None,
        direction_deg=_mean_direction(hours),
        peak_region=str(peak["region_id"]),
        peak_time=to_local(peak["time_utc"]),
    )


def _verdict(
    wet: bool, thunder: bool, wind: NightWind | None, has_alerts: bool, cfg: BotConfig
) -> str:
    night = cfg.params.night
    windy = wind is not None and wind.max_kmh >= night.quiet_wind_max_kmh
    gusty = (
        wind is not None
        and wind.max_gust_kmh is not None
        and wind.max_gust_kmh >= night.quiet_gust_max_kmh
    )
    if not (wet or thunder or windy or gusty or has_alerts):
        return "😴 Sakin bir gece bekleniyor: yağış yok, rüzgâr hafif."
    parts = []
    if thunder:
        parts.append("⛈️ gök gürültülü sağanak")
    elif wet:
        parts.append("🌧️ yağmurlu")
    if windy and wind is not None:
        b = pick_wind_bin(wind.max_kmh, night.wind_bins)
        parts.append(f"{b.emoji} {b.label.lower()}")
    elif gusty:
        parts.append("💨 sert hamleli")
    text = "Bu gece " + (" ve ".join(parts) if parts else "dikkat") + "."
    if has_alerts:
        text += " ⚠️ Uyarı var, aşağıya bak."
    return text


def _wind_lines(wind: NightWind | None, cfg: BotConfig, today_local: datetime) -> list[str]:
    if wind is None:
        return ["Rüzgâr verisi yok."]
    b = pick_wind_bin(wind.max_kmh, cfg.params.night.wind_bins)
    d = wind.direction_deg
    head = f"{b.emoji} {escape(b.label)}"
    if d is not None:
        head += f" · {wind_arrow(d)} {compass(d)} yönlü"
    line = f"En güçlü {wind.max_kmh:.0f} km/sa ({kmh_to_knots(wind.max_kmh):.0f} kn)"
    if wind.max_gust_kmh is not None:
        line += f", hamle en çok {wind.max_gust_kmh:.0f} km/sa"
    region = cfg.region_names.get(wind.peak_region, wind.peak_region)
    where = f"En rüzgârlı: {escape(region)}, {fmt_day_time(wind.peak_time, today_local.date())}"
    return [head, line, where]


def format_night(
    rows: list[Row],
    active: list[Row],
    events: list[Row],
    cfg: BotConfig,
    now_local: datetime,
) -> str:
    """Night summary body with the full footer (disclaimer + attribution).

    rows: store.rain_hours(*query_range(*night_window(now_local, cfg))); events: upcoming
    alert events (filtered here to those starting before the end of the night).
    """
    start, end = night_window(now_local, cfg)
    today = now_local.date()
    rain_hours: list[RainHour] = [h for h in prepare_hours(rows) if start <= h.start < end]
    wet_lines = wet_ranges(rain_hours, cfg, today, start, end)
    thunder_codes = cfg.params.rain.thunder_weather_codes
    thunder = any(
        h.code in thunder_codes and h.mm >= cfg.params.rain.wet_hour_mm for h in rain_hours
    )
    wind = night_wind(rows, start, end)

    tonight = [e for e in events if to_local(e["start_utc"]) < end]
    alerts = format_alerts_section(active, tonight, cfg, now_local)
    has_alerts = alerts != no_alerts_text(cfg)
    if not has_alerts:
        alerts = "Bu gece için uyarı yok ✅"

    rain_body = region_lines(rain_hours, cfg) if rain_hours else ["Yağış verisi yok."]
    rain_part = ["<b>🌧️ Yağış</b>", *rain_body]
    if wet_lines:
        rain_part += ["", "<b>Yağışlı saatler</b>", *wet_lines]

    span = f"{weekday_tr(start)} {start:%H:%M} → {weekday_tr(end)} {end:%H:%M}"
    parts = [
        f"<b>🌙 İyi geceler! Gece özeti</b>\n{span}",
        _verdict(bool(wet_lines), thunder, wind, has_alerts, cfg),
        f"<b>Uyarılar</b>\n{alerts}",
        "\n".join(rain_part),
        "\n".join(["<b>💨 Rüzgâr</b>", *_wind_lines(wind, cfg, now_local)]),
    ]
    return with_footer("\n\n".join(parts), cfg)
