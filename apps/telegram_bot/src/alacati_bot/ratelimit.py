"""Per-chat sliding-window rate limiter."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable
from enum import Enum


class Verdict(Enum):
    """Outcome of a rate-limit check."""

    ALLOW = "allow"
    NOTIFY = "notify"  # first rejected command in the window: tell the user once
    IGNORE = "ignore"  # further rejected commands: stay silent


class RateLimiter:
    """Allow at most `limit` commands per chat within `window_s` seconds."""

    _SWEEP_AT = 1024

    def __init__(
        self, limit: int, window_s: float, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._limit = limit
        self._window = window_s
        self._clock = clock
        self._hits: dict[int, deque[float]] = {}
        self._notified: set[int] = set()

    def check(self, chat_id: int) -> Verdict:
        now = self._clock()
        if len(self._hits) > self._SWEEP_AT:
            self._sweep(now)
        hits = self._hits.setdefault(chat_id, deque())
        while hits and now - hits[0] >= self._window:
            hits.popleft()
        if len(hits) < self._limit:
            hits.append(now)
            self._notified.discard(chat_id)
            return Verdict.ALLOW
        if chat_id in self._notified:
            return Verdict.IGNORE
        self._notified.add(chat_id)
        return Verdict.NOTIFY

    def _sweep(self, now: float) -> None:
        for cid in [c for c, h in self._hits.items() if not h or now - h[-1] >= self._window]:
            del self._hits[cid]
            self._notified.discard(cid)
