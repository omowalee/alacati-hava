"""Thin Kafka layer: a consumer port (real + fakes in tests) and per-partition offset tracking.

Delivery is at-least-once: `enable.auto.commit=false`, and offsets are committed only
for messages whose side effect has finished (see OffsetTracker).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Record:
    """One consumed Kafka message (only what the consumers need)."""

    partition: int
    offset: int
    value: bytes | None
    key: bytes | None = None


class ConsumerPort(Protocol):
    def poll(self, timeout: float) -> Record | None:
        """Next record, or None (timeout / non-fatal error)."""
        ...

    def commit(self, offsets: dict[int, int]) -> None:
        """Synchronously commit `next offset to read` per partition."""
        ...

    def close(self) -> None: ...


@dataclass
class OffsetTracker:
    """Which offsets are safe to commit, per partition.

    Every consumed offset is `track`ed; it becomes `done` once its side effect is finished
    (loaded, evaluated, or skipped as invalid). The committable offset of a partition is
    the lowest offset still pending, or one past the highest seen offset when nothing is
    pending. Committing it never skips a message that still needs work.

    The first offset seen on a partition is where the group already stands (committed
    offset, or the reset policy), so it is the baseline: only progress is committed.
    """

    pending: dict[int, set[int]] = field(default_factory=dict)
    next_offset: dict[int, int] = field(default_factory=dict)
    committed: dict[int, int] = field(default_factory=dict)

    def track(self, partition: int, offset: int) -> None:
        self.committed.setdefault(partition, offset)
        self.pending.setdefault(partition, set()).add(offset)
        self.next_offset[partition] = max(self.next_offset.get(partition, 0), offset + 1)

    def done(self, partition: int, offset: int) -> None:
        self.pending.get(partition, set()).discard(offset)

    def committable(self) -> dict[int, int]:
        out: dict[int, int] = {}
        for partition, nxt in self.next_offset.items():
            pending = self.pending.get(partition)
            target = min(pending) if pending else nxt
            if target > self.committed.get(partition, -1):
                out[partition] = target
        return out

    def mark_committed(self, offsets: dict[int, int]) -> None:
        for partition, offset in offsets.items():
            self.committed[partition] = max(self.committed.get(partition, -1), offset)

    def reset(self) -> None:
        self.pending.clear()
        self.next_offset.clear()
        self.committed.clear()


def consumer_config(bootstrap_servers: str, group_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "client.id": group_id,
        "enable.auto.commit": False,
        # A new group starts at the oldest retained message: nothing is skipped, and
        # replays are harmless (deterministic job / notification ids).
        "auto.offset.reset": "earliest",
        # Eager rebalancing: on every rebalance all partitions are revoked and the
        # consumer restarts from the committed offsets, which is what on_revoke relies on.
        "partition.assignment.strategy": "range",
        "session.timeout.ms": 45000,
        "max.poll.interval.ms": 600000,
    }


class ConfluentConsumer:
    """ConsumerPort backed by confluent_kafka.Consumer, subscribed to one topic."""

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topic: str,
        on_revoke: Callable[[], None],
    ) -> None:
        from confluent_kafka import Consumer

        self._topic = topic
        self._on_revoke = on_revoke
        self._consumer = Consumer(consumer_config(bootstrap_servers, group_id))
        self._consumer.subscribe(
            [topic], on_assign=self._assigned, on_revoke=self._revoked, on_lost=self._revoked
        )

    def _assigned(self, _consumer: Any, partitions: list[Any]) -> None:
        log.info("assigned partitions %s", sorted(p.partition for p in partitions))

    def _revoked(self, _consumer: Any, partitions: list[Any]) -> None:
        log.warning(
            "partitions revoked %s: dropping in-memory buffers (they are re-read from the "
            "committed offsets)",
            sorted(p.partition for p in partitions),
        )
        self._on_revoke()

    def poll(self, timeout: float) -> Record | None:
        from confluent_kafka import KafkaError, KafkaException

        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        err = msg.error()
        if err is not None:
            if err.code() == KafkaError._PARTITION_EOF:
                return None
            if err.fatal():
                raise KafkaException(err)
            log.warning("consumer error (non-fatal): %s", err)
            return None
        partition, offset = msg.partition(), msg.offset()
        if partition is None or offset is None:
            log.warning("record without partition/offset ignored")
            return None
        return Record(partition=partition, offset=offset, value=msg.value(), key=msg.key())

    def commit(self, offsets: dict[int, int]) -> None:
        from confluent_kafka import TopicPartition

        if not offsets:
            return
        self._consumer.commit(
            offsets=[TopicPartition(self._topic, p, o) for p, o in offsets.items()],
            asynchronous=False,
        )

    def close(self) -> None:
        self._consumer.close()


class ProducerPort(Protocol):
    def publish(self, topic: str, key: str, value: bytes) -> None:
        """Queue one message (delivery is confirmed by flush)."""
        ...

    def flush(self) -> None:
        """Wait for every queued message; raise if any was not acknowledged."""
        ...


class ConfluentProducer:
    """ProducerPort: acks=all, idempotent; flush raises on any failed delivery."""

    def __init__(self, bootstrap_servers: str, client_id: str, flush_timeout_s: float = 30.0):
        from confluent_kafka import Producer

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
            }
        )
        self._flush_timeout = flush_timeout_s
        self._errors: list[str] = []

    def publish(self, topic: str, key: str, value: bytes) -> None:
        def _cb(err: Any, _msg: Any) -> None:
            if err is not None:
                self._errors.append(str(err))

        self._producer.produce(topic, key=key.encode("utf-8"), value=value, on_delivery=_cb)
        self._producer.poll(0)

    def flush(self) -> None:
        remaining = int(self._producer.flush(self._flush_timeout))
        errors, self._errors = self._errors, []
        if remaining or errors:
            raise RuntimeError(
                f"Kafka delivery failed: {remaining} undelivered, errors={errors[:3]}"
            )
