"""alacati-bq-consumer: weather.raw -> BigQuery raw tables (consumer group bq-loader).

Per record: validate against the contract (invalid -> log + skip, its offset is done at
once so a bad message never blocks the partition), buffer by run_id. When a run is
closed (complete, or `RUN_FLUSH_TIMEOUT_S` after its last message) its messages are
flattened with the ingestion loader's own code and loaded with batch load jobs. Offsets
are committed only after the load succeeded.

Restart / replay safety: a complete run is loaded under the same deterministic job ids
as the Phase 2 direct loader ('raw_<table>_<run_id>_<n>'), so a replay hits BigQuery's
Conflict check and loads nothing twice. Batches that are not a whole run (partial run,
late messages) get the job-id key '<run_id>_<digest of their message_ids>', which is
just as deterministic for the same batch. Remaining duplicates (a replay that regroups
messages differently) are removed by dbt staging, which dedupes on message_id.
"""

from __future__ import annotations

import hashlib
import logging
import signal
import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from alacati_ingestion.bq_loader import (
    RAW_TABLE_FOR_SOURCE,
    BigQueryStore,
    LoadError,
    flatten_message,
)

from alacati_streaming.kafka_io import ConsumerPort, OffsetTracker, Record
from alacati_streaming.runs import RunBuffer, RunCollector, decode

log = logging.getLogger(__name__)

# How many loaded runs to remember, so replayed messages of a run that is already in
# BigQuery are skipped instead of loaded again (memory only; staging dedupes anyway).
REMEMBER_LOADED_RUNS = 200
# A deterministic load job that failed cannot be reused (its id exists). The next attempt
# uses '<key>_retry<k>'. After this many failed ids the batch keeps backing off.
MAX_JOB_ID_ATTEMPTS = 20


class RawLoader(Protocol):
    def load_batch(self, run_key: str, messages: list[dict[str, Any]]) -> None:
        """Load all messages or raise. Same run_key + same messages = same job ids."""
        ...


class BigQueryRawLoader(BigQueryStore):
    """Reuses the ingestion loader: flatten_message + its deterministic-job-id `_load`."""

    def load_batch(self, run_key: str, messages: list[dict[str, Any]]) -> None:
        ingested_at = datetime.now(UTC)
        for source, table in RAW_TABLE_FOR_SOURCE.items():
            batch = [m for m in messages if m["source"] == source]
            if not batch:
                continue
            rows = [r for m in batch for r in flatten_message(m, ingested_at)]
            self._load_with_fresh_id_after_failure(table, run_key, rows)

    def _load_with_fresh_id_after_failure(
        self, table: str, run_key: str, rows: list[dict[str, Any]]
    ) -> None:
        # Walk the same id sequence on every attempt / replay: an id whose job succeeded
        # stops the walk (Conflict check), an id whose job FAILED (no rows were written:
        # load jobs are atomic) moves on to the next id.
        for attempt in range(MAX_JOB_ID_ATTEMPTS):
            key = run_key if attempt == 0 else f"{run_key}_retry{attempt}"
            try:
                self._load("raw", table, key, rows)
                return
            except LoadError as exc:
                log.warning("load %s/%s: earlier job failed (%s); trying next id", table, key, exc)
        raise LoadError(f"{table}: {MAX_JOB_ID_ATTEMPTS} job ids used up for {run_key}")


def batch_key(buf: RunBuffer, whole_run: bool) -> str:
    """Job-id key: the run_id for a whole run, run_id + digest for any other batch."""
    if whole_run:
        return buf.run_id
    digest = hashlib.sha256("\n".join(sorted(buf.messages)).encode("utf-8")).hexdigest()[:12]
    return f"{buf.run_id}_{digest}"


@dataclass
class _Retry:
    attempts: int
    next_try: float


class BqConsumerApp:
    def __init__(
        self,
        consumer: ConsumerPort,
        loader: RawLoader,
        flush_timeout_s: float,
        retry_backoff_s: float = 15.0,
        retry_backoff_max_s: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._consumer = consumer
        self._loader = loader
        self._clock = clock
        self._backoff = retry_backoff_s
        self._backoff_max = retry_backoff_max_s
        self.collector = RunCollector(flush_timeout_s)
        self.tracker = OffsetTracker()
        self._retry: dict[str, _Retry] = {}
        # run_id -> message_ids already in BigQuery (bounded, oldest evicted first)
        self._loaded: OrderedDict[str, set[str]] = OrderedDict()

    # --- rebalance ---------------------------------------------------------------------
    def on_revoke(self) -> None:
        """Forget everything uncommitted; the records are re-read from committed offsets."""
        self.collector.clear()
        self.tracker.reset()
        self._retry.clear()

    # --- per record ----------------------------------------------------------------------
    def handle(self, record: Record) -> None:
        self.tracker.track(record.partition, record.offset)
        message = decode(record)
        if message is None:
            self.tracker.done(record.partition, record.offset)
            return
        run_id, message_id = str(message["run_id"]), str(message["message_id"])
        if message_id in self._loaded.get(run_id, ()):
            log.info("message %s of run %s is already loaded: skipped", message_id, run_id)
            self.tracker.done(record.partition, record.offset)
            return
        self.collector.add(record, message, self._clock())

    # --- periodic ----------------------------------------------------------------------
    def flush_closed_runs(self) -> None:
        now = self._clock()
        for buf in self.collector.closed(now):
            retry = self._retry.get(buf.run_id)
            if retry and now < retry.next_try:
                continue
            whole_run = buf.complete and buf.run_id not in self._loaded
            key = batch_key(buf, whole_run)
            messages = list(buf.messages.values())
            try:
                self._loader.load_batch(key, messages)
            except Exception:
                attempts = (retry.attempts if retry else 0) + 1
                delay = min(self._backoff * 2 ** (attempts - 1), self._backoff_max)
                self._retry[buf.run_id] = _Retry(attempts, now + delay)
                log.exception(
                    "load of run %s (%d messages) failed (attempt %d); retry in %.0fs; "
                    "offsets stay uncommitted",
                    buf.run_id,
                    len(messages),
                    attempts,
                    delay,
                )
                continue
            self._retry.pop(buf.run_id, None)
            self.collector.pop(buf.run_id)
            self._remember(buf.run_id, set(buf.messages))
            for partition, offset in buf.offsets:
                self.tracker.done(partition, offset)
            log.info(
                "run %s: loaded %d/%d messages (%s, key %s)",
                buf.run_id,
                len(buf.messages),
                buf.expected,
                "complete" if buf.complete else "closed by timeout",
                key,
            )

    def _remember(self, run_id: str, message_ids: set[str]) -> None:
        self._loaded.setdefault(run_id, set()).update(message_ids)
        self._loaded.move_to_end(run_id)
        while len(self._loaded) > REMEMBER_LOADED_RUNS:
            self._loaded.popitem(last=False)

    def commit(self) -> None:
        offsets = self.tracker.committable()
        if not offsets:
            return
        self._consumer.commit(offsets)
        self.tracker.mark_committed(offsets)
        log.debug("committed %s", offsets)

    def step(self, poll_timeout: float = 1.0) -> None:
        record = self._consumer.poll(poll_timeout)
        if record is not None:
            self.handle(record)
        self.flush_closed_runs()
        self.commit()


def touch(path: Path) -> None:
    try:
        path.touch()
    except OSError:
        log.debug("heartbeat %s not writable", path)


def run_until_stopped(step: Callable[[], None], heartbeat: Path, stop: threading.Event) -> None:
    while not stop.is_set():
        step()
        touch(heartbeat)


def install_signal_handlers(stop: threading.Event) -> None:
    def _stop(signum: int, _frame: Any) -> None:
        log.info("signal %s: stopping after the current step", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )


def main() -> int:
    from alacati_ingestion.settings import bigquery_settings
    from google.cloud import bigquery

    from alacati_streaming.kafka_io import ConfluentConsumer
    from alacati_streaming.settings import BQ_LOADER_GROUP, RAW_TOPIC, ConsumerSettings

    configure_logging()
    settings = ConsumerSettings.from_env()
    bq = bigquery_settings()
    loader = BigQueryRawLoader(bigquery.Client(project=bq.project_id, location=bq.location), bq)

    holder: dict[str, BqConsumerApp] = {}
    consumer = ConfluentConsumer(
        settings.bootstrap_servers,
        BQ_LOADER_GROUP,
        RAW_TOPIC,
        on_revoke=lambda: holder["app"].on_revoke() if "app" in holder else None,
    )
    app = BqConsumerApp(
        consumer,
        loader,
        flush_timeout_s=settings.run_flush_timeout_s,
        retry_backoff_s=settings.retry_backoff_s,
        retry_backoff_max_s=settings.retry_backoff_max_s,
    )
    holder["app"] = app
    stop = threading.Event()
    install_signal_handlers(stop)
    log.info("bq-consumer started (group %s, topic %s)", BQ_LOADER_GROUP, RAW_TOPIC)
    try:
        run_until_stopped(app.step, settings.heartbeat_path, stop)
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
