"""Build Turkish HTML messages from mart rows. Pure functions, no I/O."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from html import escape as _escape
from typing import Any, Protocol

from .formatting import (
    LEVEL_EMOJI,
    QUALITY_TR,
    SPORT_TR,
    SURF_QUALITY_TR,
    SURF_SPORT_TR,
    compass,
    fmt_day_time,
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


ERROR_TEXT = "Şu an veriye ulaşamadım, lütfen biraz sonra tekrar dene."
RATE_LIMIT_TEXT = "Çok fazla istek gönderdiniz, lütfen biraz bekleyin."


class DataStore(Protocol):
    """Read access to the marts (implemented by BigQuery and by test fakes)."""

    def active_alerts(self) -> list[Row]: ...
    def upcoming_events(self, horizon_hours: int) -> list[Row]: ...
    def beach_top(self, day: date, limit: int) -> list[Row]: ...
    def wind_series(self, location_id: str, hours: int) -> list[Row]: ...
    def surf_windows(self, spot_id: str, hours: int) -> list[Row]: ...
    def forecast_run_id(self) -> str | None: ...
    def latest_fetched_at(self) -> datetime | None: ...


def no_alerts_text(cfg: BotConfig) -> str:
    return f"Önümüzdeki {cfg.horizon_hours} saatte uyarı yok ✅"


def stale_notice(store: DataStore, cfg: BotConfig, now_local: datetime) -> str:
    """Turkish warning if the newest forecast is older than data.max_age_hours, else ''."""
    fetched = store.latest_fetched_at()
    if fetched is None:
        return "⚠️ Veriler güncel değil (güncelleme bilgisi bulunamadı)"
    local = to_local(fetched)
    if now_local - local > timedelta(hours=cfg.params.max_age_hours):
        return f"⚠️ Veriler güncel değil (son güncelleme: {local:%d.%m %H:%M})"
    return ""


def with_notice(body: str, notice: str) -> str:
    return f"{notice}\n\n{body}" if notice else body


_TAG = re.compile(r"<(/?)([a-z]+)[^>]*>")


def truncate_message(text: str, cfg: BotConfig, tail: str | None = None) -> str:
    """Fit text into telegram_max_chars: cut the body at a line boundary, keep the footer.

    Tags never span lines in our messages, so a line-boundary cut cannot split a tag; any
    tag left open by a cut is closed explicitly.
    """
    limit = cfg.params.telegram_max_chars
    if len(text) <= limit:
        return text
    tail = tail if tail is not None else footer(cfg)
    if text.endswith(tail):
        body = text[: -len(tail)]
    else:
        body, tail = text, ""
    mark = "\n…"
    room = max(limit - len(tail) - len(mark) - 16, 0)  # 16: slack for closing tags
    cut = body[:room]
    nl = cut.rfind("\n")
    cut = cut[:nl] if nl > 0 else cut
    if cut.rfind("<") > cut.rfind(">"):  # no newline available: drop a dangling tag start
        cut = cut[: cut.rfind("<")]
    if cut.rfind("&") > cut.rfind(";"):
        cut = cut[: cut.rfind("&")]
    stack: list[str] = []
    for m in _TAG.finditer(cut):
        if m.group(1):
            if stack and stack[-1] == m.group(2):
                stack.pop()
        else:
            stack.append(m.group(2))
    closing = "".join(f"</{t}>" for t in reversed(stack))
    return cut + closing + mark + tail


def footer(cfg: BotConfig) -> str:
    """Disclaimer + attribution; appended to every reply."""
    return f"\n\n<i>{escape(cfg.disclaimer)}</i>\n{escape(cfg.attribution)}"


def attribution_footer(cfg: BotConfig) -> str:
    """Data attribution only (CC BY 4.0 licence requirement); used by /sorf."""
    return f"\n\n{escape(cfg.attribution)}"


def with_footer(body: str, cfg: BotConfig) -> str:
    return body + footer(cfg)


def target_beach_day(now_local: datetime, cfg: BotConfig) -> tuple[date, bool]:
    """Day to rank beaches for; from the evening switch hour on it is tomorrow."""
    if now_local.hour >= cfg.params.evening_switch_hour:
        return now_local.date() + timedelta(days=1), True
    return now_local.date(), False


def help_text(cfg: BotConfig) -> str:
    body = (
        "<b>Alaçatı Hava</b>\n"
        "Çeşme yarımadası için şiddetli hava uyarıları, plaj önerileri ve "
        "Alaçatı rüzgâr durumu.\n\n"
        "/uyari – aktif ve yaklaşan uyarılar\n"
        f"/plaj – rüzgârdan en korunaklı {cfg.params.top_beaches} plaj\n"
        "/ruzgar – Alaçatı rüzgâr tahmini\n"
        "/sorf – sörf / kite için uygun saatler (/surf da çalışır)\n"
        "/yardim – bu mesaj\n\n"
        "Her sabah günlük özet gönderilir."
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
    """Alert blocks (active + upcoming); no_alerts_text if both are empty."""
    today = now_local.date()
    blocks: list[str] = []
    if active:
        blocks.append("<b>Aktif uyarılar</b>")
        for r in active:
            blocks.append(_alert_line(r, cfg, r["start_local"], r["end_local"], today))
    if events:
        blocks.append(f"<b>Yaklaşan ({cfg.horizon_hours} saat)</b>")
        for r in events:
            blocks.append(
                _alert_line(r, cfg, to_local(r["start_utc"]), to_local(r["end_utc"]), today)
            )
    if not blocks:
        return no_alerts_text(cfg)
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


def pick_wind_steps(series: list[Row], cfg: BotConfig) -> list[Row]:
    """Every wind_step_hours-th row within wind_span_hours (series is hourly, sorted)."""
    step, span = cfg.params.wind_step_hours, cfg.params.wind_span_hours
    return [r for i, r in enumerate(series) if i % step == 0 and i <= span]


def format_windows(windows: list[Row], today: date) -> list[str]:
    lines = []
    for w in windows:
        sport = SPORT_TR.get(str(w["sport"]), str(w["sport"]))
        quality = QUALITY_TR.get(str(w["quality"]), str(w["quality"]))
        rng = fmt_range(to_local(w["window_start_utc"]), to_local(w["window_end_utc"]), today)
        lines.append(f"• {rng} · {sport} {quality} · ort. {float(w['avg_wind_kmh']):.0f} km/sa")
    return lines


def format_wind_section(
    series: list[Row], windows: list[Row], cfg: BotConfig, now_local: datetime
) -> str:
    steps = pick_wind_steps(series, cfg)
    parts = [f"<b>Alaçatı rüzgâr ({cfg.params.wind_span_hours} saat)</b>"]
    parts.append("\n".join(_wind_line(r) for r in steps) if steps else "Rüzgâr verisi yok.")
    parts.append("<b>Yaklaşan uygun saatler</b>")
    wl = format_windows(windows, now_local.date())
    parts.append("\n".join(wl) if wl else "Önümüzdeki saatlerde uygun pencere yok.")
    return "\n\n".join(parts)


def format_wind(series: list[Row], windows: list[Row], cfg: BotConfig, now_local: datetime) -> str:
    return with_footer(format_wind_section(series, windows, cfg, now_local), cfg)


def _marker(hour: datetime, windows: list[Row]) -> str:
    """'✅ sörf' inside an ideal window, '👍 sörf' inside a rideable one, else '—'."""
    found = ""
    for w in windows:
        if to_local(w["window_start_utc"]) <= hour < to_local(w["window_end_utc"]):
            if w["quality"] == "ideal":
                return "✅ sörf"
            if w["quality"] == "rideable":
                found = "👍 sörf"
    return found or "—"


def _surf_hour_line(r: Row, windows: list[Row], today: date) -> str:
    t = r["time_local"]
    when = f"{t:%H:%M}" if t.date() == today else fmt_day_time(t, today)
    direction = r.get("wind_direction_10m")
    speed = float(r["wind_speed_10m"])
    line = f"{when} {wind_arrow(direction)} {compass(direction)} · {kmh_to_knots(speed):.0f} kn"
    gust = r.get("wind_gusts_10m")
    if gust is not None:
        line += f", hamle {kmh_to_knots(float(gust)):.0f} kn"
    return f"{line} · {_marker(t, windows)}"


def _surf_window_line(w: Row, today: date) -> str:
    rng = fmt_range(to_local(w["window_start_utc"]), to_local(w["window_end_utc"]), today)
    quality = SURF_QUALITY_TR.get(str(w["quality"]), str(w["quality"]))
    avg = float(w["avg_wind_kmh"])
    line = f"• {rng} · {quality} · ort. {avg:.0f} km/sa ({kmh_to_knots(avg):.0f} kn)"
    gust = w.get("max_gust_kmh")
    if gust is not None:
        line += f", maks. hamle {kmh_to_knots(float(gust)):.0f} kn"
    return line


def format_surf(
    spots: list[tuple[str, str, str, list[Row], list[Row]]], cfg: BotConfig, now_local: datetime
) -> str:
    """/sorf body. spots: (id, name, sport, hourly series, windows); attribution-only footer."""
    today = now_local.date()
    blocks = ["<b>🏄 Sörf / Kite — önümüzdeki saatler</b>"]
    for _, name, sport, series, windows in spots:
        sport_tr = SURF_SPORT_TR.get(sport, sport)
        lines = [f"<b>{escape(name)}</b> · {escape(sport_tr)}", "<b>Sörf için uygun saatler:</b>"]
        if windows:
            lines += [_surf_window_line(w, today) for w in windows]
        else:
            lines.append("Önümüzdeki saatlerde sörf için uygun pencere yok.")
        steps = pick_wind_steps(series, cfg)
        lines.append("")
        lines += [_surf_hour_line(r, windows, today) for r in steps] or ["Rüzgâr verisi yok."]
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks) + attribution_footer(cfg)


def format_summary(
    active: list[Row],
    events: list[Row],
    beaches: list[Row],
    series: list[Row],
    windows: list[Row],
    cfg: BotConfig,
    now_local: datetime,
) -> str:
    """Morning summary: today's alerts, top beaches, Alaçatı wind."""
    end_of_today = datetime.combine(now_local.date() + timedelta(days=1), datetime.min.time())
    todays = [e for e in events if to_local(e["start_utc"]) < end_of_today]
    alerts = format_alerts_section(active, todays, cfg, now_local)
    if alerts == no_alerts_text(cfg):
        alerts = "Bugün uyarı yok ✅"
    beach = (
        "\n".join(_beach_lines(beaches[: cfg.params.summary_top_beaches]))
        if beaches
        else "Plaj verisi henüz yok."
    )
    body = (
        f"<b>☀️ Günaydın! {now_local:%d.%m.%Y} özeti</b>\n\n"
        f"<b>Uyarılar</b>\n{alerts}\n\n"
        f"<b>En iyi plajlar</b>\n{beach}\n\n"
        f"{format_wind_section(series, windows, cfg, now_local)}"
    )
    return with_footer(body, cfg)
