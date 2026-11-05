"""Test doubles: Kafka consumer/producer, clock, weather.raw message factory."""

from __future__ import annotations

import json
import uuid
from collections import deque
from typing import Any

from alacati_streaming.kafka_io import Record


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class FakeConsumer:
    def __init__(self) -> None:
        self.queue: deque[Record] = deque()
        self.commits: list[dict[int, int]] = []
        self._next: dict[int, int] = {}

    def push(self, value: bytes | dict[str, Any] | None, partition: int = 0) -> Record:
        offset = self._next.get(partition, 0)
        self._next[partition] = offset + 1
        raw = json.dumps(value).encode("utf-8") if isinstance(value, dict) else value
        rec = Record(partition=partition, offset=offset, value=raw)
        self.queue.append(rec)
        return rec

    def poll(self, timeout: float) -> Record | None:
        return self.queue.popleft() if self.queue else None

    def commit(self, offsets: dict[int, int]) -> None:
        self.commits.append(dict(offsets))

    def close(self) -> None:
        pass

    @property
    def committed(self) -> dict[int, int]:
        out: dict[int, int] = {}
        for c in self.commits:
            out.update(c)
        return out


class FakeProducer:
    def __init__(self, calls: list[str] | None = None) -> None:
        self.published: list[tuple[str, str, dict[str, Any]]] = []
        self.fail_flush = False
        self.calls = calls if calls is not None else []
        self._queued: list[tuple[str, str, dict[str, Any]]] = []

    def publish(self, topic: str, key: str, value: bytes) -> None:
        self._queued.append((topic, key, json.loads(value)))

    def flush(self) -> None:
        self.calls.append("flush")
        queued, self._queued = self._queued, []
        if self.fail_flush:
            raise RuntimeError("broker down")
        self.published.extend(queued)


def raw_message(
    run_id: str,
    expected: int,
    point_id: str = "alacati_surf",
    source: str = "forecast",
    message_id: str | None = None,
) -> dict[str, Any]:
    hour: dict[str, Any] = {"time_utc": "2026-11-05T10:00:00Z"}
    hour.update({"wind_speed_10m": 20.0} if source == "forecast" else {"wave_height": 1.0})
    return {
        "schema_version": 1,
        "message_id": message_id or str(uuid.uuid4()),
        "run_id": run_id,
        "run_expected_messages": expected,
        "source": source,
        "point_id": point_id,
        "fetched_at": "2026-11-05T10:05:00Z",
        "request": {"latitude": 38.27, "longitude": 26.37, "forecast_days": 3, "past_hours": 3},
        "grid": {"latitude": 38.27, "longitude": 26.37, "elevation_m": 4.0},
        "model": "best_match",
        "units": {"wind_speed_10m": "km/h"},
        "hourly": [hour],
    }


def new_run_id() -> str:
    return str(uuid.uuid4())
