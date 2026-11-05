"""Formatting, after-19:00 switch and summary idempotency (no network, no BigQuery)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from alacati_bot import bot, settings
from alacati_bot.messages import (
    NO_ALERTS,
    Row,
    format_summary,
    help_text,
    target_beach_day,
)
from alacati_bot.store import TTLCache
from alacati_bot.summary import run_summary

CFG = settings.load_config()
NOW = datetime(2026, 11, 5, 10, 0)  # local


class FakeStore:
    def __init__(self, **kw: Any) -> None:
        self.active: list[Row] = kw.get("active", [])
        self.events: list[Row] = kw.get("events", [])
        self.beaches: list[Row] = kw.get("beaches", [])
        self.wind: list[Row] = kw.get("wind", [])
        self.windows: list[Row] = kw.get("windows", [])
        self.run_id: str | None = kw.get("run_id", "run-1")
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
    assert target_beach_day(datetime(2026, 11, 5, 18, 59)) == (date(2026, 11, 5), False)
    assert target_beach_day(datetime(2026, 11, 5, 19, 0)) == (date(2026, 11, 6), True)
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
    assert run_summary(store, sender, CFG, "1", datetime.now(UTC)) == 1
    assert not sender.messages and not store.records


def test_summary_send_failure_not_recorded() -> None:
    class Boom:
        def send(self, chat_id: str, text: str) -> int | None:
            raise RuntimeError("telegram down")

    store = FakeStore()
    with pytest.raises(RuntimeError):
        run_summary(store, Boom(), CFG, "1", datetime.now(UTC))
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
