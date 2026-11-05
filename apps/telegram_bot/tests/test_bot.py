"""Formatting, after-19:00 switch and summary idempotency (no network, no BigQuery)."""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from alacati_bot import bot, settings
from alacati_bot.messages import (
    RATE_LIMIT_TEXT,
    Row,
    format_summary,
    help_text,
    no_alerts_text,
    target_beach_day,
    truncate_message,
)
from alacati_bot.ratelimit import RateLimiter, Verdict
from alacati_bot.store import TTLCache
from alacati_bot.summary import run_summary

CFG = settings.load_config()
NOW = datetime(2026, 11, 5, 10, 0)  # local
FRESH = datetime(2026, 11, 5, 6, 0, tzinfo=UTC)  # 09:00 local, 1 h before NOW
NO_ALERTS = no_alerts_text(CFG)


class FakeStore:
    def __init__(self, **kw: Any) -> None:
        self.active: list[Row] = kw.get("active", [])
        self.events: list[Row] = kw.get("events", [])
        self.beaches: list[Row] = kw.get("beaches", [])
        self.wind: list[Row] = kw.get("wind", [])
        self.windows: list[Row] = kw.get("windows", [])
        self.run_id: str | None = kw.get("run_id", "run-1")
        self.fetched: datetime | None = kw.get("fetched", FRESH)
        self.record_failures: int = kw.get("record_failures", 0)  # -1 = always fail
        self.record_calls = 0
        self.sent: set[tuple[date, str]] = set()
        self.records: list[tuple[date, str, int | None, str]] = []
        self.beach_days: list[date] = []

    def active_alerts(self) -> list[Row]:
        return self.active

    def upcoming_events(self, horizon_hours: int) -> list[Row]:
        return self.events

    def beach_top(self, day: date, limit: int) -> list[Row]:
        self.beach_days.append(day)
        return self.beaches[:limit]

    def wind_series(self, location_id: str, hours: int) -> list[Row]:
        return self.wind

    def surf_windows(self, spot_id: str, hours: int) -> list[Row]:
        return self.windows

    def forecast_run_id(self) -> str | None:
        return self.run_id

    def latest_fetched_at(self) -> datetime | None:
        return self.fetched

    def summary_sent(self, summary_date: date, chat_id: str) -> bool:
        return (summary_date, chat_id) in self.sent

    def record_summary(
        self,
        summary_date: date,
        chat_id: str,
        sent_at: datetime,
        message_id: int | None,
        forecast_run_id: str,
    ) -> None:
        self.record_calls += 1
        if self.record_failures < 0 or self.record_calls <= self.record_failures:
            raise RuntimeError("bigquery down")
        self.sent.add((summary_date, chat_id))
        self.records.append((summary_date, chat_id, message_id, forecast_run_id))


class FakeSender:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def send(self, chat_id: str, text: str) -> int | None:
        self.messages.append(text)
        return 42


def _event() -> Row:
    return {
        "hazard": "strong_wind",
        "level": "orange",
        "start_utc": datetime(2026, 11, 5, 12, 0, tzinfo=UTC),
        "end_utc": datetime(2026, 11, 5, 18, 0, tzinfo=UTC),
        "peak_value": 83.0,
        "peak_unit": "km/h",
        "affected_regions": ["alacati", "ilica"],
    }


def _beaches(n: int = 5) -> list[Row]:
    return [
        {
            "location_name": f"Plaj {i}",
            "avg_score": 80.0 - i,
            "best_hour_local": datetime(2026, 11, 5, 14, 0),
            "rank_in_day": i,
            "label_tr": "Çok iyi",
            "shore_verified": i != 2,
        }
        for i in range(1, n + 1)
    ]


def _wind() -> list[Row]:
    return [
        {
            "time_local": datetime(2026, 11, 5, 10 + i, 0),
            "wind_speed_10m": 30.0 + i,
            "wind_gusts_10m": 45.0,
            "wind_direction_10m": 350.0,
        }
        for i in range(13)
    ]


def _window() -> Row:
    return {
        "spot_id": "alacati_surf",
        "sport": "windsurf",
        "quality": "ideal",
        "window_start_utc": datetime(2026, 11, 5, 9, 0, tzinfo=UTC),
        "window_end_utc": datetime(2026, 11, 5, 13, 0, tzinfo=UTC),
        "avg_wind_kmh": 35.0,
    }


def _assert_footer(text: str) -> None:
    assert CFG.disclaimer in text
    assert CFG.attribution in text
    assert "CC BY 4.0" in text


def test_help_has_commands_and_footer() -> None:
    text = help_text(CFG)
    for cmd in ("/uyari", "/plaj", "/ruzgar", "/yardim"):
        assert cmd in text
    _assert_footer(text)


def test_alerts_empty() -> None:
    text = bot.reply_alerts(FakeStore(), CFG, NOW)
    assert NO_ALERTS in text
    _assert_footer(text)


def test_alerts_with_event() -> None:
    text = bot.reply_alerts(FakeStore(events=[_event()]), CFG, NOW)
    assert "🟠" in text
    assert CFG.hazard_labels["strong_wind"] in text
    assert "15:00–21:00" in text  # UTC+3
    assert "83" in text and "km/h" in text
    assert "Alaçatı" in text and "Ilıca" in text
    assert NO_ALERTS not in text
    _assert_footer(text)


def test_beaches_empty_and_full() -> None:
    _assert_footer(bot.reply_beaches(FakeStore(), CFG, NOW))
    text = bot.reply_beaches(FakeStore(beaches=_beaches()), CFG, NOW)
    assert "Bugün" in text
    assert text.count("(yön doğrulanmadı)") == 1
    assert "14:00" in text
    _assert_footer(text)


def test_after_19_switches_to_tomorrow() -> None:
    assert target_beach_day(datetime(2026, 11, 5, 18, 59), CFG) == (date(2026, 11, 5), False)
    assert target_beach_day(datetime(2026, 11, 5, 19, 0), CFG) == (date(2026, 11, 6), True)
    store = FakeStore(beaches=_beaches())
    text = bot.reply_beaches(store, CFG, datetime(2026, 11, 5, 21, 0))
    assert store.beach_days == [date(2026, 11, 6)]
    assert "Yarın" in text


def test_wind_steps_and_windows() -> None:
    text = bot.reply_wind(FakeStore(wind=_wind(), windows=[_window()]), CFG, NOW)
    for hh in ("10:00", "13:00", "16:00", "19:00", "22:00"):
        assert hh in text
    assert "11:00" not in text
    assert "kn" in text and "hamle" in text and "K " in text
    assert "Windsurf ideal" in text
    _assert_footer(text)


def test_wind_empty() -> None:
    text = bot.reply_wind(FakeStore(), CFG, NOW)
    assert "Rüzgâr verisi yok" in text
    _assert_footer(text)


def test_summary_content() -> None:
    text = format_summary([], [_event()], _beaches(), _wind(), [], CFG, NOW)
    assert "🟠" in text
    assert text.count("Plaj ") == 3
    assert "Alaçatı rüzgâr" in text
    _assert_footer(text)


def test_summary_empty_alerts() -> None:
    text = format_summary([], [], [], [], [], CFG, NOW)
    assert "Bugün uyarı yok" in text
    _assert_footer(text)


def test_summary_idempotent() -> None:
    store, sender = FakeStore(beaches=_beaches()), FakeSender()
    now = datetime(2026, 11, 5, 5, 0, tzinfo=UTC)
    assert run_summary(store, sender, CFG, "123", now) == 0
    assert run_summary(store, sender, CFG, "123", now) == 0
    assert len(sender.messages) == 1
    assert store.records == [(date(2026, 11, 5), "123", 42, "run-1")]
    # next local day sends again; other chat too
    assert run_summary(store, sender, CFG, "123", datetime(2026, 11, 6, 5, 0, tzinfo=UTC)) == 0
    assert run_summary(store, sender, CFG, "999", now) == 0
    assert len(sender.messages) == 3


def test_summary_local_date_uses_istanbul() -> None:
    store, sender = FakeStore(), FakeSender()
    run_summary(store, sender, CFG, "1", datetime(2026, 11, 5, 22, 0, tzinfo=UTC))
    assert store.records[0][0] == date(2026, 11, 6)


def test_summary_no_run_id_is_error() -> None:
    store, sender = FakeStore(run_id=None), FakeSender()
    assert run_summary(store, sender, CFG, "1", datetime(2026, 11, 5, 5, 0, tzinfo=UTC)) == 1
    assert not sender.messages and not store.records


def test_summary_send_failure_not_recorded() -> None:
    class Boom:
        def send(self, chat_id: str, text: str) -> int | None:
            raise RuntimeError("telegram down")

    store = FakeStore()
    with pytest.raises(RuntimeError):
        run_summary(store, Boom(), CFG, "1", datetime(2026, 11, 5, 5, 0, tzinfo=UTC))
    assert not store.records


def test_dry_run_neither_sends_nor_writes(capsys: pytest.CaptureFixture[str]) -> None:
    store = FakeStore()
    assert run_summary(store, None, CFG, "1", datetime.now(UTC), dry_run=True) == 0
    assert CFG.attribution in capsys.readouterr().out
    assert not store.records


def test_ttl_cache() -> None:
    t = [0.0]
    cache = TTLCache(ttl=600, clock=lambda: t[0])
    calls: list[int] = []

    def fn() -> int:
        calls.append(1)
        return len(calls)

    assert cache.get_or_set("k", fn) == 1
    t[0] = 599
    assert cache.get_or_set("k", fn) == 1
    t[0] = 601
    assert cache.get_or_set("k", fn) == 2


# --- review fixes ---------------------------------------------------------------------

EARLY = datetime(2026, 11, 5, 5, 0, tzinfo=UTC)  # 08:00 local


def test_record_fails_then_succeeds_sends_once() -> None:
    store, sender = FakeStore(record_failures=2), FakeSender()
    sleeps: list[float] = []
    assert run_summary(store, sender, CFG, "1", EARLY, sleep=sleeps.append) == 0
    assert len(sender.messages) == 1
    assert store.record_calls == 3
    assert sleeps == [CFG.params.record_backoff_s] * 2


def test_record_always_fails_returns_3_and_sends_once() -> None:
    store, sender = FakeStore(record_failures=-1), FakeSender()
    sleeps: list[float] = []
    assert run_summary(store, sender, CFG, "1", EARLY, sleep=sleeps.append) == 3
    assert len(sender.messages) == 1
    assert store.record_calls == CFG.params.record_retries + 1


def test_send_timeout_is_not_resent() -> None:
    calls: list[str] = []

    class Slow:
        def send(self, chat_id: str, text: str) -> int | None:
            calls.append(text)
            raise TimeoutError("read timeout")

    store = FakeStore()
    with pytest.raises(TimeoutError):
        run_summary(store, Slow(), CFG, "1", EARLY)
    assert len(calls) == 1 and not store.records


def test_summary_too_late_not_sent() -> None:
    store, sender = FakeStore(), FakeSender()
    late = datetime(2026, 11, 5, 10, 0, tzinfo=UTC)  # 13:00 local
    assert run_summary(store, sender, CFG, "1", late) == 0
    assert not sender.messages


def test_stale_warning_on_every_reply() -> None:
    old = datetime(2026, 11, 5, 11, 0, tzinfo=UTC)  # 14:00 local
    now = datetime(2026, 11, 5, 18, 30)
    store = FakeStore(fetched=old, beaches=_beaches(), wind=_wind())
    for fn in (bot.reply_alerts, bot.reply_beaches, bot.reply_wind):
        text = fn(store, CFG, now)
        assert text.startswith("⚠️ Veriler güncel değil (son güncelleme: 05.11 14:00)")
        _assert_footer(text)
    fresh = bot.reply_alerts(FakeStore(), CFG, NOW)
    assert "Veriler güncel değil" not in fresh


def test_stale_warning_in_summary() -> None:
    store, sender = FakeStore(fetched=FRESH - timedelta(hours=10)), FakeSender()
    assert run_summary(store, sender, CFG, "1", EARLY) == 0
    assert sender.messages[0].startswith("⚠️ Veriler güncel değil")


def test_missing_fetch_time_warns() -> None:
    assert "Veriler güncel değil" in bot.reply_alerts(FakeStore(fetched=None), CFG, NOW)


def test_rate_limiter_notifies_once_then_ignores() -> None:
    t = [0.0]
    lim = RateLimiter(3, 60, clock=lambda: t[0])
    assert [lim.check(1) for _ in range(3)] == [Verdict.ALLOW] * 3
    assert lim.check(1) is Verdict.NOTIFY
    assert lim.check(1) is Verdict.IGNORE
    assert lim.check(2) is Verdict.ALLOW  # other chats unaffected
    t[0] = 61
    assert lim.check(1) is Verdict.ALLOW


def _update(chat_id: int, sent: list[str]) -> Any:
    async def reply_text(text: str, **kw: Any) -> None:
        sent.append(text)

    return SimpleNamespace(
        message=SimpleNamespace(reply_text=reply_text), effective_chat=SimpleNamespace(id=chat_id)
    )


def test_handler_rate_limit_replies_once() -> None:
    lim = RateLimiter(2, 60)
    handler = bot._make_handler(bot.reply_alerts, FakeStore(), CFG, lim)
    sent: list[str] = []
    for _ in range(5):
        asyncio.run(handler(_update(7, sent), cast(Any, None)))
    assert len(sent) == 3
    assert RATE_LIMIT_TEXT in sent[2]
    _assert_footer(sent[2])


def test_handler_error_and_timeout_give_generic_text() -> None:
    class Broken(FakeStore):
        def active_alerts(self) -> list[Row]:
            raise RuntimeError("bq down")

    sent: list[str] = []
    handler = bot._make_handler(bot.reply_alerts, Broken(), CFG, RateLimiter(10, 60))
    asyncio.run(handler(_update(1, sent), cast(Any, None)))
    assert "veriye ulaşamadım" in sent[0]
    _assert_footer(sent[0])


def test_truncate_keeps_footer_and_tags() -> None:
    rows = [f"<b>Satır {i}</b> " + "x" * 100 for i in range(100)]
    body = "<i>\n" + "\n".join(rows)
    text = body + "\n\n<i>" + CFG.disclaimer + "</i>\n" + CFG.attribution
    out = truncate_message(text, CFG)
    assert len(out) <= CFG.params.telegram_max_chars
    _assert_footer(out)
    assert out.count("<i>") == out.count("</i>")
    assert out.count("<b>") == out.count("</b>")
    assert truncate_message("kısa", CFG) == "kısa"


def test_truncate_real_message() -> None:
    events = [_event() for _ in range(80)]
    out = bot.reply_alerts(FakeStore(events=events), CFG, NOW)
    assert len(out) <= CFG.params.telegram_max_chars
    _assert_footer(out)


def _surf_window(quality: str, start_h: int, end_h: int) -> Row:
    return {
        "spot_id": "alacati_surf",
        "sport": "windsurf",
        "quality": quality,
        "window_start_utc": datetime(2026, 11, 5, start_h, 0, tzinfo=UTC),
        "window_end_utc": datetime(2026, 11, 5, end_h, 0, tzinfo=UTC),
        "avg_wind_kmh": 35.0,
        "max_gust_kmh": 50.0,
    }


def _assert_surf_footer(text: str) -> None:
    assert CFG.attribution in text
    assert CFG.disclaimer not in text
    assert "MGM" not in text


def test_surf_with_windows_and_markers() -> None:
    # local = UTC+3: ideal 10:00-13:00 local, rideable 13:00-16:00 local
    store = FakeStore(
        wind=_wind(),
        windows=[_surf_window("ideal", 7, 10), _surf_window("rideable", 10, 13)],
    )
    text = bot.reply_surf(store, CFG, NOW)
    assert "🏄 Sörf / Kite — önümüzdeki saatler" in text
    assert "Rüzgâr sörfü" in text and "Kite" in text
    assert "Sörf için uygun saatler:" in text
    assert "✅ ideal" in text and "👍 sörf yapılabilir" in text
    assert "maks. hamle 27 kn" in text
    lines = {ln[:5]: ln for ln in text.splitlines() if ln[:2].isdigit() and " kn" in ln}
    assert lines["10:00"].endswith("✅ sörf")
    assert lines["13:00"].endswith("👍 sörf")
    assert lines["16:00"].endswith("—")
    _assert_surf_footer(text)


def test_surf_without_windows() -> None:
    text = bot.reply_surf(FakeStore(wind=_wind()), CFG, NOW)
    assert "Önümüzdeki saatlerde sörf için uygun pencere yok." in text
    assert "✅ sörf" not in text and "👍 sörf" not in text
    _assert_surf_footer(text)


def test_surf_stale_notice_and_truncation() -> None:
    old = FakeStore(wind=_wind(), fetched=datetime(2026, 11, 4, 0, 0, tzinfo=UTC))
    assert bot.reply_surf(old, CFG, NOW).startswith("⚠️ Veriler güncel değil")
    big = FakeStore(wind=_wind() * 400)
    cfg = settings.BotConfig(
        CFG.alerts,
        CFG.locations,
        CFG.surf,
        dataclasses.replace(CFG.params, wind_span_hours=5000, wind_step_hours=1),
    )
    out = bot.reply_surf(big, cfg, NOW)
    assert len(out) <= CFG.params.telegram_max_chars
    assert out.endswith(CFG.attribution)
    _assert_surf_footer(out)


def test_surf_alias_registered_and_help() -> None:
    app = bot.build_application("123:abc", FakeStore(), CFG)
    cmds = {c for h in app.handlers[0] for c in getattr(h, "commands", ())}
    assert {"sorf", "surf", "ruzgar"} <= cmds
    assert "/sorf" in help_text(CFG)


def test_surf_handler_error_has_no_disclaimer() -> None:
    class Broken(FakeStore):
        def wind_series(self, location_id: str, hours: int) -> list[Row]:
            raise RuntimeError("bq down")

    sent: list[str] = []
    handler = bot._make_handler(bot.reply_surf, Broken(), CFG, RateLimiter(10, 60), True)
    asyncio.run(handler(_update(1, sent), cast(Any, None)))
    assert "veriye ulaşamadım" in sent[0]
    _assert_surf_footer(sent[0])
