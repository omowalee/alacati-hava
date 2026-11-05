"""Build Turkish HTML messages from mart rows. Pure functions, no I/O."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from html import escape as _escape
from typing import Any, Protocol

from .formatting import (
    LEVEL_EMOJI,
    QUALITY_TR,
    SPORT_TR,
    compass,
    fmt_number,
    fmt_range,
    kmh_to_knots,
    to_local,
    wind_arrow,
)
from .settings import BotConfig

Row = dict[str, Any]


def escape(text: str) -> str:
    """Escape for Telegram HTML (quotes are left alone)."""
    return _escape(text, quote=False)


EVENING_SWITCH_HOUR = 19
NO_ALERTS = "Önümüzdeki 48 saatte uyarı yok ✅"
ERROR_TEXT = "Şu an veriye ulaşamadım, lütfen biraz sonra tekrar dene."
WIND_STEP_HOURS = 3
WIND_SPAN_HOURS = 12


class DataStore(Protocol):
    """Read access to the marts (implemented by BigQuery and by test fakes)."""

    def active_alerts(self) -> list[Row]: ...
    def upcoming_events(self, horizon_hours: int) -> list[Row]: ...
    def beach_top(self, day: date, limit: int) -> list[Row]: ...
    def wind_series(self, location_id: str, hours: int) -> list[Row]: ...
    def surf_windows(self, spot_id: str, hours: int) -> list[Row]: ...
    def forecast_run_id(self) -> str | None: ...


def footer(cfg: BotConfig) -> str:
    """Disclaimer + attribution; appended to every reply."""
    return f"\n\n<i>{escape(cfg.disclaimer)}</i>\n{escape(cfg.attribution)}"


def with_footer(body: str, cfg: BotConfig) -> str:
    return body + footer(cfg)


def target_beach_day(now_local: datetime) -> tuple[date, bool]:
    """Day to rank beaches for; after 19:00 local it is tomorrow. Returns (day, is_tomorrow)."""
    if now_local.hour >= EVENING_SWITCH_HOUR:
        return now_local.date() + timedelta(days=1), True
    return now_local.date(), False


def help_text(cfg: BotConfig) -> str:
    body = (
        "<b>Alaçatı Hava</b>\n"
        "Çeşme yarımadası için şiddetli hava uyarıları, plaj önerileri ve "
        "Alaçatı rüzgâr durumu.\n\n"
        "/uyari – aktif ve yaklaşan uyarılar\n"
        "/plaj – rüzgârdan en korunaklı 5 plaj\n"
        "/ruzgar – Alaçatı rüzgâr tahmini\n"
        "/yardim – bu mesaj\n\n"
        "Her sabah 08:00'de günlük özet gönderilir."
    )
    return with_footer(body, cfg)


def _regions(row: Row, cfg: BotConfig) -> str:
    names = cfg.region_names
    return ", ".join(escape(names.get(r, r)) for r in (row.get("affected_regions") or []))


def _peak(row: Row) -> str:
    value = row.get("peak_value")
    if value is None:
        return ""
    text = fmt_number(float(value), str(row.get("peak_unit") or "")).strip()
    return f"zirve {escape(text)}"


def _alert_line(row: Row, cfg: BotConfig, start: datetime, end: datetime, today: date) -> str:
    label = cfg.hazard_labels.get(str(row["hazard"]), str(row["hazard"]))
    emoji = LEVEL_EMOJI.get(str(row["level"]), "⚪")
    parts = [f"{emoji} <b>{escape(label)}</b>", fmt_range(start, end, today)]
    peak = _peak(row)
    if peak:
        parts.append(peak)
    regions = _regions(row, cfg)
    if regions:
        parts.append(regions)
    return "\n".join(parts)


def format_alerts_section(
    active: list[Row], events: list[Row], cfg: BotConfig, now_local: datetime
) -> str:
    """Alert blocks (active + upcoming); NO_ALERTS if both are empty."""
    today = now_local.date()
    blocks: list[str] = []
    if active:
        blocks.append("<b>Aktif uyarılar</b>")
        for r in active:
            blocks.append(_alert_line(r, cfg, r["start_local"], r["end_local"], today))
    if events:
        blocks.append("<b>Yaklaşan (48 saat)</b>")
        for r in events:
            blocks.append(
                _alert_line(r, cfg, to_local(r["start_utc"]), to_local(r["end_utc"]), today)
            )
    if not blocks:
        return NO_ALERTS
    return "\n\n".join(blocks)


def format_alerts(active: list[Row], events: list[Row], cfg: BotConfig, now_local: datetime) -> str:
    return with_footer(format_alerts_section(active, events, cfg, now_local), cfg)


def _beach_lines(rows: list[Row]) -> list[str]:
    lines = []
    for r in rows:
        mark = "" if r.get("shore_verified") else " <i>(yön doğrulanmadı)</i>"
        best = r["best_hour_local"]
        lines.append(
            f"{r['rank_in_day']}. <b>{escape(str(r['location_name']))}</b>{mark}\n"
            f"    {escape(str(r['label_tr']))} · skor {float(r['avg_score']):.0f} · "
            f"en iyi saat {best:%H:%M}"
        )
    return lines


def format_beaches(rows: list[Row], is_tomorrow: bool, cfg: BotConfig) -> str:
    title = "Yarın için en iyi plajlar" if is_tomorrow else "Bugün için en iyi plajlar"
    if not rows:
        body = f"<b>{title}</b>\nPlaj verisi henüz yok."
    else:
        body = "\n".join([f"<b>{title}</b>", *_beach_lines(rows)])
    return with_footer(body, cfg)


def _wind_line(r: Row) -> str:
    speed = float(r["wind_speed_10m"])
    direction = r.get("wind_direction_10m")
    line = (
        f"{r['time_local']:%H:%M} {wind_arrow(direction)} {compass(direction)} · "
        f"{speed:.0f} km/sa ({kmh_to_knots(speed):.0f} kn)"
    )
    gust = r.get("wind_gusts_10m")
    if gust is not None:
        line += f", hamle {float(gust):.0f} ({kmh_to_knots(float(gust)):.0f} kn)"
    return line


def pick_wind_steps(series: list[Row]) -> list[Row]:
    """Every WIND_STEP_HOURS-th row within WIND_SPAN_HOURS (series is hourly, sorted)."""
    return [r for i, r in enumerate(series) if i % WIND_STEP_HOURS == 0 and i <= WIND_SPAN_HOURS]


def format_windows(windows: list[Row], today: date) -> list[str]:
    lines = []
    for w in windows:
        sport = SPORT_TR.get(str(w["sport"]), str(w["sport"]))
        quality = QUALITY_TR.get(str(w["quality"]), str(w["quality"]))
        rng = fmt_range(to_local(w["window_start_utc"]), to_local(w["window_end_utc"]), today)
        lines.append(f"• {rng} · {sport} {quality} · ort. {float(w['avg_wind_kmh']):.0f} km/sa")
    return lines


def format_wind_section(series: list[Row], windows: list[Row], now_local: datetime) -> str:
    steps = pick_wind_steps(series)
    parts = ["<b>Alaçatı rüzgâr (12 saat)</b>"]
    parts.append("\n".join(_wind_line(r) for r in steps) if steps else "Rüzgâr verisi yok.")
    parts.append("<b>Yaklaşan uygun saatler</b>")
    wl = format_windows(windows, now_local.date())
    parts.append("\n".join(wl) if wl else "Önümüzdeki saatlerde uygun pencere yok.")
    return "\n\n".join(parts)


def format_wind(series: list[Row], windows: list[Row], cfg: BotConfig, now_local: datetime) -> str:
    return with_footer(format_wind_section(series, windows, now_local), cfg)


def format_summary(
    active: list[Row],
    events: list[Row],
    beaches: list[Row],
    series: list[Row],
    windows: list[Row],
    cfg: BotConfig,
    now_local: datetime,
) -> str:
    """Morning summary: today's alerts, top 3 beaches, Alaçatı wind."""
    end_of_today = datetime.combine(now_local.date() + timedelta(days=1), datetime.min.time())
    todays = [e for e in events if to_local(e["start_utc"]) < end_of_today]
    alerts = format_alerts_section(active, todays, cfg, now_local)
    if alerts == NO_ALERTS:
        alerts = "Bugün uyarı yok ✅"
    beach = "\n".join(_beach_lines(beaches[:3])) if beaches else "Plaj verisi henüz yok."
    body = (
        f"<b>☀️ Günaydın! {now_local:%d.%m.%Y} özeti</b>\n\n"
        f"<b>Uyarılar</b>\n{alerts}\n\n"
        f"<b>En iyi plajlar</b>\n{beach}\n\n"
        f"{format_wind_section(series, windows, now_local)}"
    )
    return with_footer(body, cfg)
