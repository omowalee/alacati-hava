"""Decode weather.raw records and group them by ingestion run.

A run is *closed* when it is complete (distinct message_ids == run_expected_messages) or
when no message of it arrived for `flush_timeout_s` (partial run, or a producer that
died half-way). Both consumers use this; only the payload they keep differs.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from alacati_ingestion.messages import MessageError, validate_message

from alacati_streaming.kafka_io import Record

log = logging.getLogger(__name__)


def decode(record: Record) -> dict[str, Any] | None:
    """Parse and validate against contracts/kafka/weather_raw.v1; None (logged) if invalid."""
    where = f"partition {record.partition} offset {record.offset}"
    if record.value is None:
        log.error("skipping %s: empty value (tombstone)", where)
        return None
    try:
        message = json.loads(record.value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        log.error("skipping %s: not UTF-8 JSON (%s)", where, exc)
        return None
    if not isinstance(message, dict):
        log.error("skipping %s: JSON value is not an object", where)
        return None
    try:
        validate_message(message)
    except MessageError as exc:
        log.error("skipping %s: violates weather.raw v1 (%s)", where, exc)
        return None
    return message


@dataclass
class RunBuffer:
    run_id: str
    expected: int
    first_seen: float
    last_seen: float
    messages: dict[str, dict[str, Any]] = field(default_factory=dict)  # message_id -> msg
    offsets: list[tuple[int, int]] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return len(self.messages) >= self.expected


class RunCollector:
    """Buffers validated messages per run_id until the run is closed."""

    def __init__(self, flush_timeout_s: float, keep_payload: bool = True) -> None:
        self._timeout = flush_timeout_s
        self._keep_payload = keep_payload
        self._runs: dict[str, RunBuffer] = {}

    def __len__(self) -> int:
        return len(self._runs)

    def add(self, record: Record, message: dict[str, Any], now: float) -> RunBuffer:
        run_id = str(message["run_id"])
        expected = int(message["run_expected_messages"])
        buf = self._runs.get(run_id)
        if buf is None:
            buf = RunBuffer(run_id=run_id, expected=expected, first_seen=now, last_seen=now)
            self._runs[run_id] = buf
        elif expected != buf.expected:
            log.warning(
                "run %s: run_expected_messages %d != %d seen earlier; keeping the first",
                run_id,
                expected,
                buf.expected,
            )
        buf.last_seen = now
        buf.offsets.append((record.partition, record.offset))
        # A duplicate (producer retry / replay) only adds its offset.
        buf.messages.setdefault(str(message["message_id"]), message if self._keep_payload else {})
        return buf

    def closed(self, now: float) -> list[RunBuffer]:
        """Closed runs, oldest first (not removed; call pop once handled)."""
        ready = [b for b in self._runs.values() if b.complete or now - b.last_seen >= self._timeout]
        return sorted(ready, key=lambda b: b.first_seen)

    def pop(self, run_id: str) -> RunBuffer | None:
        return self._runs.pop(run_id, None)

    def clear(self) -> None:
        self._runs.clear()
