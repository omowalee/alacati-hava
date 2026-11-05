"""Kafka sink: publish validated weather.raw v1 messages instead of loading BigQuery directly.

The sink is a RunStore: run bookkeeping (budget, last complete run, api_call_log, ingest_runs)
still goes to BigQuery through the wrapped store; only `load_raw` is replaced. Raw rows are
written later by the bq-consumer, so ingest_runs reflects fetch + publish success.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from datetime import datetime
from typing import Any, Protocol

from alacati_ingestion.open_meteo import CallRecord
from alacati_ingestion.runner import RunStore, RunSummary

log = logging.getLogger(__name__)

TOPIC = "weather.raw"
DEFAULT_FLUSH_TIMEOUT_S = 30.0

DeliveryCallback = Callable[[Exception | None], None]


class MessageProducer(Protocol):
    """Minimal producer interface (implemented by ConfluentProducer and by test fakes)."""

    def produce(self, topic: str, key: str, value: bytes, on_delivery: DeliveryCallback) -> None:
        """Queue one message; on_delivery(None) on broker ack, on_delivery(error) on failure."""
        ...

    def flush(self, timeout: float) -> int:
        """Wait for outstanding deliveries; return how many are still undelivered."""
        ...


class ConfluentProducer:
    """MessageProducer backed by confluent-kafka (acks=all, idempotent)."""

    def __init__(self, bootstrap_servers: str) -> None:
        from confluent_kafka import Producer

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "client.id": "alacati-ingest",
            }
        )

    def produce(self, topic: str, key: str, value: bytes, on_delivery: DeliveryCallback) -> None:
        def _cb(err: Any, _msg: Any) -> None:
            on_delivery(None if err is None else RuntimeError(str(err)))

        self._producer.produce(topic, key=key.encode("utf-8"), value=value, on_delivery=_cb)
        self._producer.poll(0)

    def flush(self, timeout: float) -> int:
        return int(self._producer.flush(timeout))


def message_key(message: dict[str, Any]) -> str:
    return f"{message['source']}:{message['point_id']}"


class KafkaSinkStore:
    """RunStore that publishes raw messages to Kafka and delegates ops to `ops_store`."""

    def __init__(
        self,
        producer: MessageProducer,
        ops_store: RunStore,
        topic: str = TOPIC,
        flush_timeout_s: float = DEFAULT_FLUSH_TIMEOUT_S,
    ) -> None:
        self._producer = producer
        self._ops = ops_store
        self._topic = topic
        self._flush_timeout = flush_timeout_s

    def todays_weighted_calls(self, now: datetime) -> float:
        return self._ops.todays_weighted_calls(now)

    def last_complete_run_started_at(self) -> datetime | None:
        return self._ops.last_complete_run_started_at()

    def load_ops(self, calls: list[CallRecord], summary: RunSummary) -> None:
        self._ops.load_ops(calls, summary)

    def load_raw(self, run_id: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Publish messages; return only those whose delivery was confirmed by the broker."""
        delivered: set[str] = set()
        for message in messages:
            message_id = str(message["message_id"])

            def on_delivery(err: Exception | None, mid: str = message_id) -> None:
                if err is None:
                    delivered.add(mid)
                else:
                    log.error("delivery failed for message %s: %s", mid, err)

            try:
                value = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
                self._producer.produce(
                    self._topic, message_key(message), value.encode("utf-8"), on_delivery
                )
            except Exception:
                log.exception("could not queue message %s", message_id)
        remaining = self._producer.flush(self._flush_timeout)
        if remaining:
            log.error("%d message(s) not delivered within %.0fs", remaining, self._flush_timeout)
        confirmed = [m for m in messages if str(m["message_id"]) in delivered]
        log.info("published %d/%d messages to %s", len(confirmed), len(messages), self._topic)
        return confirmed


def flush_timeout_from_env() -> float:
    raw = os.environ.get("KAFKA_FLUSH_TIMEOUT_S")
    return float(raw) if raw else DEFAULT_FLUSH_TIMEOUT_S
