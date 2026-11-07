"""Morning (08:00) and night (21:30) summaries: each kind is sent at most once per local
day, idempotent via ops.summary_log (unique key: summary_date, chat_id, summary_kind)."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Literal, Protocol

from . import settings
from .formatting import TZ
from .messages import DataStore, format_summary, stale_notice, truncate_message, with_notice
from .night import format_night, night_window, query_range

log = logging.getLogger("alacati_bot.summary")

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_SENT_NOT_RECORDED = 3  # sent to Telegram, summary_log write failed; do NOT retry
EXIT_NOT_FRESH = 4  # --check-fresh: the newest forecast is older than fresh_max_age_minutes

Kind = Literal["morning", "night"]
KINDS: tuple[Kind, ...] = ("morning", "night")


class SummaryStore(DataStore, Protocol):
    """DataStore plus summary_log access."""

    def summary_sent(self, summary_date: date, chat_id: str, kind: str) -> bool: ...
    def record_summary(
        self,
        summary_date: date,
        chat_id: str,
        sent_at: datetime,
        message_id: int | None,
        forecast_run_id: str,
        kind: str,
    ) -> None: ...


class Sender(Protocol):
    """Sends text to the chat and returns Telegram's message_id."""

    def send(self, chat_id: str, text: str) -> int | None: ...


def build_summary(store: DataStore, cfg: settings.BotConfig, now_local: datetime) -> str:
    """Collect mart data and render the morning summary (stale warning first, if any)."""
    p = cfg.params
    body = format_summary(
        store.active_alerts(),
        store.upcoming_events(cfg.horizon_hours),
        store.beach_top(now_local.date(), p.summary_top_beaches),
        store.wind_series(cfg.surf_spot_id, p.wind_span_hours + 1),
        store.surf_windows(cfg.surf_spot_id, p.surf_window_hours),
        cfg,
        now_local,
    )
    return truncate_message(with_notice(body, stale_notice(store, cfg, now_local)), cfg)


def build_night(store: DataStore, cfg: settings.BotConfig, now_local: datetime) -> str:
    """Collect mart data and render the night summary (stale warning first, if any)."""
    start, end = night_window(now_local, cfg)
    body = format_night(
        store.rain_hours(*query_range(start, end)),
        store.active_alerts(),
        store.upcoming_events(cfg.horizon_hours),
        cfg,
        now_local,
    )
    return truncate_message(with_notice(body, stale_notice(store, cfg, now_local)), cfg)


def build(kind: Kind, store: DataStore, cfg: settings.BotConfig, now_local: datetime) -> str:
    if kind == "night":
        return build_night(store, cfg, now_local)
    return build_summary(store, cfg, now_local)


def out_of_hours(kind: Kind, cfg: settings.BotConfig, now_local: datetime) -> str | None:
    """Why this kind must not be sent at now_local, or None if it may be sent."""
    if kind == "morning" and now_local.hour >= cfg.params.summary_stale_after_hour:
        return f"too late ({now_local:%H:%M}) for the morning summary"
    if kind == "night" and now_local.hour < cfg.params.night.earliest_local_hour:
        return f"too early ({now_local:%H:%M}) for the night summary"
    return None


def check_fresh(store: DataStore, cfg: settings.BotConfig, now_utc: datetime) -> int:
    """EXIT_OK if the newest forecast was fetched within fresh_max_age_minutes, else
    EXIT_NOT_FRESH (also when no fetch time is known)."""
    fetched = store.latest_fetched_at()
    limit = timedelta(minutes=cfg.params.fresh_max_age_minutes)
    if fetched is None:
        log.info("no forecast fetch time in the marts yet")
        return EXIT_NOT_FRESH
    if fetched.tzinfo is None:
        fetched = fetched.replace(tzinfo=UTC)
    age = now_utc - fetched
    if age > limit:
        log.info("newest forecast is %s old (limit %s): not fresh yet", age, limit)
        return EXIT_NOT_FRESH
    log.info("newest forecast is %s old: fresh", age)
    return EXIT_OK


def _record_with_retries(
    store: SummaryStore,
    cfg: settings.BotConfig,
    args: tuple[date, str, datetime, int | None, str, str],
    sleep: Callable[[float], None],
) -> bool:
    """Write summary_log, retrying record_retries times after the first failure."""
    p = cfg.params
    attempts = p.record_retries + 1
    for attempt in range(1, attempts + 1):
        try:
            store.record_summary(*args)
            return True
        except Exception:
            log.exception("record_summary failed (attempt %d/%d)", attempt, attempts)
            if attempt < attempts:
                sleep(p.record_backoff_s)
    return False


def run_summary(
    store: SummaryStore,
    sender: Sender | None,
    cfg: settings.BotConfig,
    chat_id: str,
    now_utc: datetime,
    dry_run: bool = False,
    sleep: Callable[[float], None] = time.sleep,
    kind: Kind = "morning",
) -> int:
    """Send today's summary of this kind at most once.

    Returns the exit code: 0 sent / already sent / too late to send; 1 error (nothing was
    sent, safe to retry); 3 sent but summary_log could not be written (do NOT retry: a retry
    would send a duplicate). The message is never re-sent inside this function, even if the
    send raised a timeout (Telegram may have accepted it).
    """
    now_local = now_utc.astimezone(TZ).replace(tzinfo=None)
    day = now_local.date()
    if dry_run:
        print(build(kind, store, cfg, now_local))
        return EXIT_OK
    if sender is None:
        log.error("no sender configured")
        return EXIT_ERROR
    if store.summary_sent(day, chat_id, kind):
        log.info("%s summary for %s already sent; nothing to do", kind, day)
        return EXIT_OK
    reason = out_of_hours(kind, cfg, now_local)
    if reason:
        log.warning("%s summary for %s not sent: %s", kind, day, reason)
        return EXIT_OK
    run_id = store.forecast_run_id()
    if run_id is None:
        log.error("no forecast_run_id in marts; refusing to send summary")
        return EXIT_ERROR
    text = build(kind, store, cfg, now_local)
    message_id = sender.send(chat_id, text)  # exactly one attempt
    log.info("%s summary for %s sent (message_id=%s)", kind, day, message_id)
    record = (day, chat_id, datetime.now(UTC), message_id, run_id, kind)
    if not _record_with_retries(store, cfg, record, sleep):
        log.critical(
            "%s summary for %s SENT (message_id=%s) but NOT recorded in summary_log; "
            "a retry would send a duplicate",
            kind,
            day,
            message_id,
        )
        return EXIT_SENT_NOT_RECORDED
    return EXIT_OK


class TelegramSender:
    """Sender backed by python-telegram-bot."""

    def __init__(self, token: str, timeout_s: float) -> None:
        self._token = token
        self._timeout = timeout_s

    def send(self, chat_id: str, text: str) -> int | None:
        from telegram import Bot
        from telegram.constants import ParseMode
        from telegram.request import HTTPXRequest

        async def _go() -> int | None:
            # Generous timeouts. python-telegram-bot never retries send_message itself; this
            # sender does not either. A timeout may mean Telegram already accepted the message:
            # the summary path never resends, but the alert listener may resend after a
            # timeout (at-least-once; a duplicate alert is possible, a lost one is not).
            request = HTTPXRequest(
                connect_timeout=self._timeout,
                read_timeout=self._timeout,
                write_timeout=self._timeout,
                pool_timeout=self._timeout,
            )
            async with Bot(self._token, request=request) as bot:
                msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
                return int(msg.message_id)

        return asyncio.run(_go())


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Send the morning or night weather summary once.",
        epilog="Exit codes: 0 sent / already sent / outside its hours; 1 error (nothing sent, "
        "safe to retry); 3 sent but not recorded in summary_log (do NOT retry: duplicate "
        "risk). With --check-fresh: 0 fresh, 4 not fresh yet, 1 error.",
    )
    parser.add_argument("--kind", choices=KINDS, default="morning", help="summary kind")
    parser.add_argument("--dry-run", action="store_true", help="print only; no send, no write")
    parser.add_argument(
        "--check-fresh",
        action="store_true",
        help="only check that the newest forecast is fresh enough to send (exit 0 / 4)",
    )
    args = parser.parse_args(argv)
    # Windows consoles default to a legacy code page that cannot print the emoji in the
    # message; Docker is already UTF-8, so this only matters for local --dry-run.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    code = 1
    try:
        settings.load_env()
        env = settings.load_settings()
        cfg = settings.load_config()
        from .store import BigQueryStore

        store = BigQueryStore(env, cfg.params)
        now = datetime.now(UTC)
        if args.check_fresh:
            code = check_fresh(store, cfg, now)
        elif args.dry_run:
            code = run_summary(
                store, None, cfg, env.broadcast_chat_id or "", now, dry_run=True, kind=args.kind
            )
        elif not env.token or not env.broadcast_chat_id:
            log.error(
                "TELEGRAM_BOT_TOKEN and TELEGRAM_BROADCAST_CHAT_ID (or TELEGRAM_CHAT_ID) "
                "are required"
            )
        else:
            code = run_summary(
                store,
                TelegramSender(env.token, cfg.params.telegram_timeout_s),
                cfg,
                env.broadcast_chat_id,
                now,
                kind=args.kind,
            )
    except Exception:
        log.exception("%s summary failed", args.kind)
        code = 1
    sys.exit(code)
