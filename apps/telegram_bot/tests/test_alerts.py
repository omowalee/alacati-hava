"""Alert formatting and the Kafka listener (fake consumer, sender and delivery store)."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from alacati_bot import settings
from alacati_bot.alerts import AlertError, format_alert, parse_alert
from alacati_bot.listener import AlertListener, KafkaMessage
from alacati_bot.settings import Env

CFG = settings.load_config()
ISSUED = datetime(2026, 11, 5, 10, 0, tzinfo=UTC)  # 13:00 local
CHAT = "-100"
NID = "11111111-1111-5111-8111-111111111111"


def alert(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "schema_version": 1,
        "notification_id": NID,
        "event_id": "22222222-2222-4222-8222-222222222222",
        "revision": 1,
        "status": "new",
        "change_reasons": ["first_detected"],
        "hazard": "strong_wind",
        "level": "orange",
        "previous_level": None,
        "start_utc": "2026-11-05T11:00:00Z",  # 14:00 local
        "end_utc": "2026-11-05T15:00:00Z",  # 18:00 local
        "previous_start_utc": None,
        "previous_end_utc": None,
        "peak_value": 82.4,
        "peak_unit": "km/h",
        "peak_time_utc": "2026-11-05T13:00:00Z",  # 16:00 local
        "affected_regions": ["alacati"],
        "region_levels": [{"region_id": "alacati", "level": "orange"}],
        "affected_locations": ["alacati_surf"],
        "forecast_run_id": "33333333-3333-4333-8333-333333333333",
        "issued_at": "2026-11-05T10:00:00Z",
    }
    base.update(over)
    return base


def payload(a: dict[str, Any]) -> bytes:
    return json.dumps(a).encode()


def test_schema_accepts_fixture() -> None:
    parse_alert(payload(alert()))


def test_schema_rejects_garbage() -> None:
    with pytest.raises(AlertError):
        parse_alert(b"not json")
    with pytest.raises(AlertError):
        parse_alert(payload(alert(status="bogus")))


def test_format_new() -> None:
    text = format_alert(alert(), CFG)
    assert "🆕 Yeni uyarı" in text
    assert "🟠" in text and "Turuncu seviye" in text
    assert CFG.hazard_labels["strong_wind"] in text
    assert "Bugün 14:00–18:00" in text
    assert "82 km/h (16:00)" in text
    assert "Meteoroloji" in text
    assert "CC BY 4.0" in text
    assert "•" not in text


def test_format_updated_with_reasons_and_region_levels() -> None:
    regions = list(CFG.region_names)[:2]
    a = alert(
        status="updated",
        revision=2,
        level="red",
        previous_level="orange",
        previous_start_utc="2026-11-05T11:00:00Z",
        previous_end_utc="2026-11-05T15:00:00Z",
        start_utc="2026-11-05T12:00:00Z",
        change_reasons=["level_up", "window_shift", "region_added"],
        affected_regions=regions,
        region_levels=[
            {"region_id": regions[0], "level": "red"},
            {"region_id": regions[1], "level": "yellow"},
        ],
    )
    text = format_alert(a, CFG)
    names = CFG.region_names
    assert "🔄 Uyarı güncellendi" in text
    assert "🔴" in text
    assert "• seviye yükseldi: turuncu → kırmızı" in text
    assert "• zaman aralığı değişti: önceki Bugün 14:00–18:00" in text
    assert "• yeni bölge eklendi" in text
    assert f"{names[regions[0]]} (Kırmızı)" in text
    assert f"{names[regions[1]]} (Sarı)" in text


def test_format_ended() -> None:
    a = alert(
        status="ended",
        revision=3,
        previous_level="orange",
        previous_start_utc="2026-11-05T11:00:00Z",
        previous_end_utc="2026-11-05T15:00:00Z",
        change_reasons=["window_passed"],
    )
    text = format_alert(a, CFG)
    assert "✅ Uyarı sona erdi" in text
    assert "• uyarı süresi sona erdi" in text
    assert len(text) <= CFG.params.telegram_max_chars


# --- listener ----------------------------------------------------------------------------


class FakeMsg:
    def __init__(self, data: bytes | None) -> None:
        self._data = data

    def value(self) -> bytes | None:
        return self._data

    def error(self) -> Any:
        return None


class FakeConsumer:
    def __init__(self, msgs: list[FakeMsg], events: list[str], stop: threading.Event) -> None:
        self.msgs, self.events, self.stop = msgs, events, stop
        self.closed = False

    def poll(self, timeout: float) -> KafkaMessage | None:
        if not self.msgs:
            self.stop.set()
            return None
        return self.msgs.pop(0)

    def commit(self, *, message: KafkaMessage, asynchronous: bool) -> None:
        assert asynchronous is False
        self.events.append("commit")

    def close(self) -> None:
        self.closed = True


class FakeStore:
    def __init__(self, events: list[str], record_failures: int = 0) -> None:
        self.events = events
        self.delivered: set[tuple[str, str]] = set()
        self.record_failures = record_failures  # -1 = always fail
        self.record_calls = 0

    def alert_delivered(self, notification_id: str, chat_id: str) -> bool:
        return (notification_id, chat_id) in self.delivered

    def record_alert_delivery(
        self, notification_id: str, chat_id: str, sent_at: datetime, message_id: int | None
    ) -> None:
        self.record_calls += 1
        if self.record_failures < 0 or self.record_calls <= self.record_failures:
            raise RuntimeError("bq down")
        self.events.append("record")
        self.delivered.add((notification_id, chat_id))


class FakeSender:
    def __init__(self, events: list[str], failures: int = 0) -> None:
        self.events, self.failures = events, failures
        self.sent: list[tuple[str, str]] = []

    def send(self, chat_id: str, text: str) -> int | None:
        if self.failures > 0:
            self.failures -= 1
            raise RuntimeError("telegram down")
        self.events.append("send")
        self.sent.append((chat_id, text))
        return 42


class Rig:
    def __init__(
        self, msgs: list[bytes], sender_failures: int = 0, record_failures: int = 0
    ) -> None:
        self.events: list[str] = []
        self.waits: list[float] = []
        stop = threading.Event()
        self.consumer = FakeConsumer([FakeMsg(m) for m in msgs], self.events, stop)
        self.store = FakeStore(self.events, record_failures)
        self.sender = FakeSender(self.events, sender_failures)
        self.listener = AlertListener(
            self.consumer,
            self.store,
            self.sender,
            CFG,
            CHAT,
            stop=stop,
            now=lambda: ISSUED + timedelta(minutes=5),
            wait=self._wait,
        )

    def _wait(self, seconds: float) -> bool:
        self.waits.append(seconds)
        return False


def test_send_record_commit_order() -> None:
    rig = Rig([payload(alert())])
    rig.listener.run()
    assert rig.events == ["send", "record", "commit"]
    assert rig.sender.sent[0][0] == CHAT
    assert rig.consumer.closed
    assert (NID, CHAT) in rig.store.delivered


def test_stale_message_dropped_and_committed() -> None:
    rig = Rig([payload(alert(issued_at="2026-11-05T07:00:00Z"))])
    rig.listener.run()
    assert rig.sender.sent == []
    assert rig.events == ["commit"]


def test_duplicate_skipped_but_committed() -> None:
    rig = Rig([payload(alert())])
    rig.store.delivered.add((NID, CHAT))
    rig.listener.run()
    assert rig.sender.sent == []
    assert rig.events == ["commit"]


def test_invalid_message_dropped_and_committed() -> None:
    rig = Rig([b"garbage"])
    rig.listener.run()
    assert rig.sender.sent == [] and rig.events == ["commit"]


def test_telegram_failure_retries_before_commit() -> None:
    rig = Rig([payload(alert())], sender_failures=2)
    rig.listener.run()
    assert rig.events == ["send", "record", "commit"]
    assert rig.waits == [5.0, 10.0]
    assert len(rig.sender.sent) == 1


def test_shutdown_during_retry_does_not_commit() -> None:
    rig = Rig([payload(alert())], sender_failures=99)
    rig.listener._wait = lambda _s: True  # SIGTERM arrives while backing off
    rig.listener.run()
    assert "commit" not in rig.events
    assert rig.consumer.closed


def test_record_failure_still_commits(caplog: pytest.LogCaptureFixture) -> None:
    rig = Rig([payload(alert())], record_failures=-1)
    rig.listener.run()
    assert len(rig.sender.sent) == 1  # sent exactly once
    assert rig.store.record_calls == CFG.params.record_retries + 1
    assert rig.events == ["send", "commit"]
    assert any(r.levelname == "CRITICAL" for r in caplog.records)


def test_broadcast_chat_fallback() -> None:
    kw: dict[str, Any] = {"project": None, "bq_location": "EU"}
    kw.update(marts_dataset="m", ops_dataset="o", token="t", chat_id="1")
    assert Env(**kw, broadcast_chat="2").broadcast_chat_id == "2"
    assert Env(**kw, broadcast_chat=None).broadcast_chat_id == "1"
    assert Env(**kw, broadcast_chat="").broadcast_chat_id == "1"
