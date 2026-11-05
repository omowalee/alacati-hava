"""alacati-alert-engine: weather.raw -> lifecycle -> weather.alerts (consumer group alert-engine).

The engine only uses weather.raw as a *trigger*: it tracks which runs arrived. When a run
is closed (complete, or RUN_FLUSH_TIMEOUT_S after its last message), it waits — without
blocking the Kafka poll loop — until
  1. ops.ingest_runs has the run: not 'complete' -> skipped (a partial run never drives
     alerts or absent counting, see contracts/bigquery/tables.yaml complete_run_rule);
  2. dbt has built it: mart_location_weather_hourly comes from this run or a newer one,
     and mart_alert_events was rebuilt after that run finished;
then evaluates the lifecycle (lifecycle.decide, config/alerts.yaml steps 3-4) on the
marts. Nothing is evaluated twice for the same mart run.

Write order per evaluation (each step is idempotent, so a crash anywhere is safe on
replay — the same inputs give the same event ids, revisions and notification ids):
  a. MERGE ops.alert_notifications (insert unknown notification_ids)
  b. produce weather.alerts (acks=all, idempotent producer, flush) — a replay may publish
     a notification twice; the bot dedupes on notification_id via ops.alert_deliveries
  c. MERGE ops.alert_state — after this, a replay of the run is a no-op
     (rows carry last_seen_run_id = run; absent_runs is derived from the run list)
  d. commit the Kafka offsets of the run
"""

from __future__ import annotations

import json
import logging
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import cache
from typing import Any, Protocol

from jsonschema import Draft202012Validator

from alacati_streaming.alert_store import MartRun, RunInfo
from alacati_streaming.kafka_io import ConsumerPort, OffsetTracker, ProducerPort, Record
from alacati_streaming.lifecycle import (
    AlertState,
    Decision,
    Episode,
    LifecycleConfig,
    Notification,
    decide,
)
from alacati_streaming.runs import RunCollector, decode
from alacati_streaming.settings import ALERT_SCHEMA_PATH, ALERTS_TOPIC

log = logging.getLogger(__name__)

ALERTS_TABLE = "mart_alert_events"


class EngineStore(Protocol):
    def run_info(self, run_id: str) -> RunInfo | None: ...
    def mart_run(self) -> MartRun | None: ...
    def table_modified(self, table: str) -> datetime | None: ...
    def episodes(self) -> list[Episode]: ...
    def active_states(self) -> list[AlertState]: ...
    def complete_runs(self, up_to: datetime) -> list[str]: ...
    def merge_notifications(self, notes: list[Notification]) -> None: ...
    def merge_states(self, states: list[AlertState]) -> None: ...


# --- weather.alerts encoding -----------------------------------------------------------
def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_opt(ts: datetime | None) -> str | None:
    return None if ts is None else _iso(ts)


@cache
def _alert_validator() -> Draft202012Validator:
    schema = json.loads(ALERT_SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


def alert_message(note: Notification) -> dict[str, Any]:
    """weather.alerts v1 message; raises ValueError if it violates the contract."""
    message: dict[str, Any] = {
        "schema_version": 1,
        "notification_id": note.notification_id,
        "event_id": note.event_id,
        "revision": note.revision,
        "status": note.status,
        "change_reasons": list(note.change_reasons),
        "hazard": note.hazard,
        "level": note.level,
        "previous_level": note.previous_level,
        "start_utc": _iso(note.start_utc),
        "end_utc": _iso(note.end_utc),
        "previous_start_utc": _iso_opt(note.previous_start_utc),
        "previous_end_utc": _iso_opt(note.previous_end_utc),
        "peak_value": note.peak_value,
        "peak_unit": note.peak_unit,
        "peak_time_utc": _iso(note.peak_time_utc),
        "affected_regions": list(note.affected_regions),
        "region_levels": [{"region_id": r.region_id, "level": r.level} for r in note.region_levels],
        "affected_locations": list(note.affected_locations),
        "forecast_run_id": note.forecast_run_id,
        "issued_at": _iso(note.issued_at),
    }
    errors = sorted(_alert_validator().iter_errors(message), key=lambda e: list(e.absolute_path))
    if errors:
        path = "/".join(str(p) for p in errors[0].absolute_path) or "<root>"
        raise ValueError(f"weather.alerts v1 violation at {path}: {errors[0].message}")
    return message


# --- engine ----------------------------------------------------------------------------
class NotReady(Exception):
    """The marts do not (yet) reflect the run: check again later."""


@dataclass
class PendingRun:
    run_id: str
    closed_at: float
    offsets: list[tuple[int, int]] = field(default_factory=list)
    next_check: float = 0.0
    failures: int = 0
    info: RunInfo | None = None


class AlertEngineApp:
    def __init__(
        self,
        consumer: ConsumerPort,
        store: EngineStore,
        producer: ProducerPort,
        load_config: Callable[[], LifecycleConfig],
        *,
        flush_timeout_s: float,
        mart_poll_interval_s: float,
        mart_wait_timeout_s: float,
        retry_backoff_s: float = 15.0,
        retry_backoff_max_s: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._consumer = consumer
        self._store = store
        self._producer = producer
        self._load_config = load_config
        self._poll_interval = mart_poll_interval_s
        self._wait_timeout = mart_wait_timeout_s
        self._backoff = retry_backoff_s
        self._backoff_max = retry_backoff_max_s
        self._clock = clock
        self.collector = RunCollector(flush_timeout_s, keep_payload=False)
        self.tracker = OffsetTracker()
        self.pending: dict[str, PendingRun] = {}
        self.last_evaluated_run: str | None = None

    def on_revoke(self) -> None:
        self.collector.clear()
        self.tracker.reset()
        self.pending.clear()

    def handle(self, record: Record) -> None:
        self.tracker.track(record.partition, record.offset)
        message = decode(record)
        if message is None:
            self.tracker.done(record.partition, record.offset)
            return
        self.collector.add(record, message, self._clock())

    def _close_runs(self) -> None:
        now = self._clock()
        for buf in self.collector.closed(now):
            self.collector.pop(buf.run_id)
            p = self.pending.get(buf.run_id)
            if p is None:
                p = self.pending[buf.run_id] = PendingRun(buf.run_id, closed_at=now)
                log.info(
                    "run %s closed (%d/%d messages): waiting for ingest_runs + dbt",
                    buf.run_id,
                    len(buf.messages),
                    buf.expected,
                )
            p.offsets.extend(buf.offsets)  # late messages join the pending run

    def _finish(self, p: PendingRun) -> None:
        for partition, offset in p.offsets:
            self.tracker.done(partition, offset)
        del self.pending[p.run_id]

    def process_pending(self) -> None:
        self._close_runs()
        for p in sorted(self.pending.values(), key=lambda r: r.closed_at):
            now = self._clock()
            if now < p.next_check:
                continue
            try:
                finished = self._advance(p)
            except NotReady as exc:
                if now - p.closed_at >= self._wait_timeout:
                    log.error(
                        "run %s: giving up after %.0fs (%s)", p.run_id, now - p.closed_at, exc
                    )
                    self._finish(p)
                else:
                    log.info(
                        "run %s: %s; checking again in %.0fs", p.run_id, exc, self._poll_interval
                    )
                    p.next_check = now + self._poll_interval
                continue
            except Exception:
                p.failures += 1
                delay = min(self._backoff * 2 ** (p.failures - 1), self._backoff_max)
                p.next_check = now + delay
                log.exception("run %s: evaluation failed; retry in %.0fs", p.run_id, delay)
                continue
            if finished:
                self._finish(p)

    def _advance(self, p: PendingRun) -> bool:
        """True when the run is handled (evaluated or skipped); raises NotReady to wait."""
        if p.info is None:
            info = self._store.run_info(p.run_id)
            if info is None or info.status == "running":
                raise NotReady("not in ops.ingest_runs yet")
            p.info = info
        if p.info.status != "complete":
            log.info("run %s is %s: not used for alerts", p.run_id, p.info.status)
            return True
        mart = self._store.mart_run()
        if mart is None or mart.run.started_at < p.info.started_at:
            raise NotReady("dbt has not built this run yet")
        modified = self._store.table_modified(ALERTS_TABLE)
        built_after = mart.run.finished_at or mart.run.started_at
        if modified is None or modified < built_after:
            raise NotReady(f"{ALERTS_TABLE} is older than run {mart.run.run_id}")
        if mart.run.run_id == self.last_evaluated_run:
            log.info("run %s: mart run %s already evaluated", p.run_id, mart.run.run_id)
            return True
        self.evaluate(mart, modified)
        return True

    def evaluate(self, mart: MartRun, built_at: datetime) -> Decision:
        run_id = mart.run.run_id
        episodes = self._store.episodes()
        stale = {e.forecast_run_id for e in episodes} - {run_id}
        if stale:
            raise NotReady(f"{ALERTS_TABLE} is from run(s) {sorted(stale)}, not {run_id}")
        runs = self._store.complete_runs(mart.run.started_at)
        if not runs or runs[-1] != run_id:
            raise NotReady(f"run {run_id} is not the newest complete run in ops.ingest_runs")
        states = self._store.active_states()
        # dbt build time = the "now" the mart clipped its episodes at; deterministic for
        # a replay of the same build (whole seconds, as in the Kafka message).
        now = built_at.astimezone(UTC).replace(microsecond=0)
        decision = decide(
            episodes,
            states,
            run_id=run_id,
            complete_runs=runs,
            now=now,
            config=self._load_config(),
        )
        messages = [alert_message(n) for n in decision.notifications]  # validate first
        self._store.merge_notifications(decision.notifications)
        for note, message in zip(decision.notifications, messages, strict=True):
            value = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
            self._producer.publish(ALERTS_TOPIC, note.event_id, value.encode("utf-8"))
        if messages:
            self._producer.flush()
        self._store.merge_states(decision.states)
        self.last_evaluated_run = run_id
        log.info(
            "evaluated run %s: %d episode(s), %d active event(s), %d notification(s) %s, "
            "%d state row(s) written",
            run_id,
            len(episodes),
            len(states),
            len(decision.notifications),
            [f"{n.hazard}:{n.status}:{','.join(n.change_reasons)}" for n in decision.notifications],
            len(decision.states),
        )
        return decision

    def commit(self) -> None:
        offsets = self.tracker.committable()
        if offsets:
            self._consumer.commit(offsets)
            self.tracker.mark_committed(offsets)

    def step(self, poll_timeout: float = 1.0) -> None:
        record = self._consumer.poll(poll_timeout)
        if record is not None:
            self.handle(record)
        self.process_pending()
        self.commit()


def main() -> int:
    from alacati_ingestion.settings import bigquery_settings
    from google.cloud import bigquery

    from alacati_streaming.alert_store import AlertStore
    from alacati_streaming.bq_consumer import (
        configure_logging,
        install_signal_handlers,
        run_until_stopped,
    )
    from alacati_streaming.kafka_io import ConfluentConsumer, ConfluentProducer
    from alacati_streaming.settings import (
        ALERT_ENGINE_GROUP,
        RAW_TOPIC,
        ConsumerSettings,
        EngineTimings,
        load_yaml_config,
    )

    configure_logging()
    settings = ConsumerSettings.from_env()
    timings = EngineTimings.from_env()

    def load_config() -> LifecycleConfig:
        return LifecycleConfig.from_alerts_yaml(load_yaml_config("alerts"))

    load_config()  # fail fast on a bad config/alerts.yaml
    bq = bigquery_settings()
    store = AlertStore(bigquery.Client(project=bq.project_id, location=bq.location), bq)
    producer = ConfluentProducer(settings.bootstrap_servers, client_id=ALERT_ENGINE_GROUP)

    holder: dict[str, AlertEngineApp] = {}
    consumer = ConfluentConsumer(
        settings.bootstrap_servers,
        ALERT_ENGINE_GROUP,
        RAW_TOPIC,
        on_revoke=lambda: holder["app"].on_revoke() if "app" in holder else None,
    )
    app = AlertEngineApp(
        consumer,
        store,
        producer,
        load_config,
        flush_timeout_s=settings.run_flush_timeout_s,
        mart_poll_interval_s=timings.mart_poll_interval_s,
        mart_wait_timeout_s=timings.mart_wait_timeout_s,
        retry_backoff_s=settings.retry_backoff_s,
        retry_backoff_max_s=settings.retry_backoff_max_s,
    )
    holder["app"] = app
    stop = threading.Event()
    install_signal_handlers(stop)
    log.info(
        "alert-engine started (group %s, topic %s -> %s)",
        ALERT_ENGINE_GROUP,
        RAW_TOPIC,
        ALERTS_TOPIC,
    )
    try:
        run_until_stopped(app.step, settings.heartbeat_path, stop)
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
