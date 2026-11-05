"""bq-consumer buffering and commit behaviour (fake Kafka, fake loader)."""

from __future__ import annotations

from typing import Any

import pytest
from fakes import FakeClock, FakeConsumer, new_run_id, raw_message

from alacati_streaming.bq_consumer import BqConsumerApp, batch_key
from alacati_streaming.kafka_io import OffsetTracker


class FakeLoader:
    def __init__(self) -> None:
        self.loads: list[tuple[str, list[str]]] = []
        self.fail = 0  # fail the next N calls

    def load_batch(self, run_key: str, messages: list[dict[str, Any]]) -> None:
        if self.fail:
            self.fail -= 1
            raise RuntimeError("BigQuery unavailable")
        self.loads.append((run_key, sorted(m["message_id"] for m in messages)))


def make_app(timeout: float = 120.0) -> tuple[BqConsumerApp, FakeConsumer, FakeLoader, FakeClock]:
    consumer, loader, clock = FakeConsumer(), FakeLoader(), FakeClock()
    app = BqConsumerApp(consumer, loader, flush_timeout_s=timeout, retry_backoff_s=10, clock=clock)
    return app, consumer, loader, clock


def drain(app: BqConsumerApp, consumer: FakeConsumer) -> None:
    while consumer.queue:
        app.step(0)
    app.step(0)


def test_complete_run_is_loaded_once_with_the_run_id_key_then_committed() -> None:
    app, consumer, loader, _ = make_app()
    run = new_run_id()
    consumer.push(raw_message(run, 3, "a"), partition=0)
    consumer.push(raw_message(run, 3, "b"), partition=1)
    app.step(0)
    app.step(0)
    assert loader.loads == [] and consumer.committed == {}  # not complete yet
    consumer.push(raw_message(run, 3, "c", source="marine"), partition=0)
    drain(app, consumer)
    assert [key for key, _ in loader.loads] == [run]
    assert consumer.committed == {0: 2, 1: 1}


def test_failed_load_commits_nothing_and_is_retried_after_backoff() -> None:
    app, consumer, loader, clock = make_app()
    loader.fail = 1
    run = new_run_id()
    consumer.push(raw_message(run, 1))
    drain(app, consumer)
    assert loader.loads == [] and consumer.committed == {}
    app.step(0)  # still inside the back-off window
    assert loader.loads == []
    clock.advance(11)
    app.step(0)
    assert [key for key, _ in loader.loads] == [run]
    assert consumer.committed == {0: 1}


def test_invalid_messages_are_skipped_without_blocking_the_partition() -> None:
    app, consumer, loader, _ = make_app()
    consumer.push(b"not json")
    bad = raw_message(new_run_id(), 1)
    del bad["hourly"]
    consumer.push(bad)
    consumer.push(None)
    drain(app, consumer)
    assert loader.loads == []
    assert consumer.committed == {0: 3}


def test_commit_never_passes_a_message_still_buffered() -> None:
    app, consumer, loader, _ = make_app()
    run_a, run_b = new_run_id(), new_run_id()
    consumer.push(raw_message(run_a, 2))  # p0 o0: run A waits for a 2nd message
    consumer.push(raw_message(run_b, 1))  # p0 o1: run B complete
    drain(app, consumer)
    assert [key for key, _ in loader.loads] == [run_b]
    assert consumer.committed == {}  # o0 is still pending, so nothing can be committed
    consumer.push(raw_message(run_a, 2, "x"))
    drain(app, consumer)
    assert consumer.committed == {0: 3}


def test_incomplete_run_is_flushed_after_timeout_with_a_digest_key() -> None:
    app, consumer, loader, clock = make_app(timeout=120)
    run = new_run_id()
    consumer.push(raw_message(run, 3, "a"))
    consumer.push(raw_message(run, 3, "b"))
    drain(app, consumer)
    assert loader.loads == []
    clock.advance(121)
    app.step(0)
    ((key, ids),) = loader.loads
    assert key.startswith(f"{run}_") and len(ids) == 2
    assert consumer.committed == {0: 2}


def test_duplicates_are_loaded_once_and_replays_of_loaded_runs_are_skipped() -> None:
    app, consumer, loader, _ = make_app()
    run = new_run_id()
    msg = raw_message(run, 2, "a")
    consumer.push(msg)
    consumer.push(msg)  # producer retry duplicate
    consumer.push(raw_message(run, 2, "b"))
    drain(app, consumer)
    ((key, ids),) = loader.loads
    assert key == run and len(ids) == 2
    consumer.push(msg)  # replay of an already loaded message
    drain(app, consumer)
    assert len(loader.loads) == 1
    assert consumer.committed == {0: 4}


def test_revoke_drops_buffers_so_records_are_reread_from_committed_offsets() -> None:
    app, consumer, loader, _ = make_app()
    consumer.push(raw_message(new_run_id(), 2))
    drain(app, consumer)
    assert len(app.collector) == 1
    app.on_revoke()
    assert len(app.collector) == 0 and app.tracker.committable() == {}


def test_batch_key_is_deterministic_for_the_same_messages() -> None:
    from alacati_streaming.runs import RunBuffer

    a = RunBuffer("run", 3, 0, 0, messages={"m2": {}, "m1": {}})
    b = RunBuffer("run", 3, 0, 0, messages={"m1": {}, "m2": {}})
    assert batch_key(a, whole_run=False) == batch_key(b, whole_run=False) != "run"
    assert batch_key(a, whole_run=True) == "run"


@pytest.mark.parametrize(
    ("ops", "expected"),
    [
        ([("t", 0, 0), ("t", 0, 1), ("d", 0, 1)], {}),
        ([("t", 0, 0), ("t", 0, 1), ("d", 0, 0)], {0: 1}),
        ([("t", 0, 0), ("d", 0, 0), ("t", 1, 5), ("d", 1, 5)], {0: 1, 1: 6}),
    ],
)
def test_offset_tracker(ops: list[tuple[str, int, int]], expected: dict[int, int]) -> None:
    tracker = OffsetTracker()
    for op, partition, offset in ops:
        (tracker.track if op == "t" else tracker.done)(partition, offset)
    assert tracker.committable() == expected
    tracker.mark_committed(expected)
    assert tracker.committable() == {}
