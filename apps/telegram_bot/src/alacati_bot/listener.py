"""alacati-bot-alerts: consume weather.alerts and broadcast each notification to Telegram.

At-least-once with idempotent delivery: the offset is committed only after the message was
handled (sent + recorded, dropped as stale/invalid, or already delivered). A Telegram or
BigQuery failure leaves the offset uncommitted and the message is retried with backoff.
Permanent Telegram errors (bad request, forbidden, invalid token) are terminal: logged at
CRITICAL and the offset is committed, so the single partition is never blocked.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from confluent_kafka import KafkaException
from telegram.error import BadRequest, ChatMigrated, Forbidden, InvalidToken, RetryAfter

from . import settings
from .alerts import AlertError, format_alert, is_stale, parse_alert
from .summary import Sender, TelegramSender

log = logging.getLogger("alacati_bot.listener")

TOPIC = "weather.alerts"
GROUP_ID = "telegram-bot"
POLL_TIMEOUT_S = 1.0
RETRY_BACKOFF_START_S = 5.0
# Cap well below max.poll.interval.ms so one stuck message cannot get the consumer evicted.
RETRY_BACKOFF_MAX_S = 60.0
MAX_POLL_INTERVAL_MS = 900_000
# Retrying these can never succeed (bad chat/token/payload): drop, log, commit.
TERMINAL_TELEGRAM_ERRORS = (BadRequest, Forbidden, InvalidToken, ChatMigrated)


class KafkaMessage(Protocol):
    def value(self) -> bytes | None: ...
    def error(self) -> Any: ...


class AlertConsumer(Protocol):
    """The slice of confluent_kafka.Consumer used here (fakes implement it in tests)."""

    def poll(self, timeout: float) -> KafkaMessage | None: ...
    def commit(self, *, message: KafkaMessage, asynchronous: bool) -> Any: ...
    def close(self) -> None: ...


class DeliveryStore(Protocol):
    """Access to ops.alert_deliveries."""

    def alert_delivered(self, notification_id: str, chat_id: str) -> bool: ...
    def record_alert_delivery(
        self, notification_id: str, chat_id: str, sent_at: datetime, message_id: int | None
    ) -> None: ...


class AlertListener:
    def __init__(
        self,
        consumer: AlertConsumer,
        store: DeliveryStore,
        sender: Sender,
        cfg: settings.BotConfig,
        chat_id: str,
        *,
        stop: threading.Event | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        wait: Callable[[float], bool] | None = None,
        contracts: Any = None,
    ) -> None:
        self._consumer = consumer
        self._store = store
        self._sender = sender
        self._cfg = cfg
        self._chat_id = chat_id
        self.stop = stop or threading.Event()
        self._now = now
        self._wait = wait or self.stop.wait
        self._contracts = contracts

    # --- handling one message --------------------------------------------------------
    def _record_with_retries(
        self, notification_id: str, sent_at: datetime, mid: int | None
    ) -> None:
        p = self._cfg.params
        attempts = p.record_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                self._store.record_alert_delivery(notification_id, self._chat_id, sent_at, mid)
                return
            except Exception:
                log.exception("record delivery failed (attempt %d/%d)", attempt, attempts)
                if attempt < attempts and self._wait(p.record_backoff_s):
                    log.info(
                        "shutdown interrupted recording of notification %s (message_id=%s); "
                        "it may be re-sent after restart unless the offset was committed",
                        notification_id,
                        mid,
                    )
                    return
        log.critical(
            "notification %s SENT (message_id=%s) but NOT recorded in alert_deliveries; "
            "committing the offset anyway to avoid a resend loop",
            notification_id,
            mid,
        )

    def _attempt(self, payload: bytes) -> None:
        """Handle one payload once. Raises if Telegram/BigQuery failed (=> do not commit)."""
        try:
            alert = parse_alert(payload, self._contracts)
        except AlertError as exc:
            log.error("dropping invalid alert message: %s", exc)
            return
        nid = str(alert["notification_id"])
        now = self._now()
        if is_stale(alert, now, self._cfg.max_notification_age_minutes):
            log.warning("dropping stale notification %s (issued_at=%s)", nid, alert["issued_at"])
            return
        if self._store.alert_delivered(nid, self._chat_id):
            log.info("notification %s already delivered to %s; skipping", nid, self._chat_id)
            return
        mid = self._sender.send(self._chat_id, format_alert(alert, self._cfg))
        log.info("notification %s (%s) sent, message_id=%s", nid, alert["status"], mid)
        self._record_with_retries(nid, self._now(), mid)

    def handle(self, payload: bytes) -> bool:
        """Handle with retry/backoff. True = handled (commit); False = interrupted by shutdown."""
        delay = RETRY_BACKOFF_START_S
        while True:
            try:
                self._attempt(payload)
                return True
            except TERMINAL_TELEGRAM_ERRORS:
                log.critical(
                    "permanent Telegram error; dropping notification and committing the offset",
                    exc_info=True,
                )
                return True
            except RetryAfter as exc:
                ra = exc.retry_after
                wait_s = ra.total_seconds() if isinstance(ra, timedelta) else float(ra)
                log.warning("Telegram flood limit; retrying in %.0fs", wait_s)
                if self._wait(wait_s):
                    return False
            except Exception:
                # TimedOut / NetworkError / BigQuery errors (and anything unexpected: an alert
                # is never dropped for an unknown reason) are retried with capped backoff.
                log.exception("delivery failed; retrying in %.0fs (offset not committed)", delay)
                if self._wait(delay):
                    return False
                delay = min(delay * 2, RETRY_BACKOFF_MAX_S)

    def _commit(self, msg: KafkaMessage) -> None:
        try:
            self._consumer.commit(message=msg, asynchronous=False)
        except KafkaException:
            # E.g. the consumer was evicted/rebalanced. The message is redelivered and
            # skipped via alert_deliveries, so continuing is safe.
            log.warning("offset commit failed; relying on delivery dedupe", exc_info=True)

    # --- main loop ---------------------------------------------------------------------
    def run(self) -> None:
        try:
            while not self.stop.is_set():
                msg = self._consumer.poll(POLL_TIMEOUT_S)
                if msg is None:
                    continue
                if msg.error():
                    log.error("kafka error: %s", msg.error())
                    continue
                payload = msg.value()
                if payload is None or self.handle(payload):
                    self._commit(msg)
        finally:
            self._consumer.close()
            log.info("alert listener stopped")


def build_consumer(bootstrap: str) -> AlertConsumer:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": MAX_POLL_INTERVAL_MS,
        }
    )
    consumer.subscribe([TOPIC])
    return cast(AlertConsumer, consumer)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    settings.load_env()
    env = settings.load_settings()
    cfg = settings.load_config()
    chat_id = env.broadcast_chat_id
    if not env.token or not chat_id or not env.kafka_bootstrap:
        log.error(
            "TELEGRAM_BOT_TOKEN, TELEGRAM_BROADCAST_CHAT_ID (or TELEGRAM_CHAT_ID) and "
            "KAFKA_BOOTSTRAP_SERVERS are required"
        )
        sys.exit(1)
    from .store import BigQueryStore

    listener = AlertListener(
        build_consumer(env.kafka_bootstrap),
        BigQueryStore(env, cfg.params),
        TelegramSender(env.token, cfg.params.telegram_timeout_s),
        cfg,
        chat_id,
    )
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda _s, _f: listener.stop.set())
    listener.run()
