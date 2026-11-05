"""Morning summary: send once per local day, idempotent via ops.summary_log."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Protocol

from . import settings
from .formatting import TZ
from .messages import DataStore, format_summary

log = logging.getLogger("alacati_bot.summary")


class SummaryStore(DataStore, Protocol):
    """DataStore plus summary_log access."""

    def summary_sent(self, summary_date: date, chat_id: str) -> bool: ...
    def record_summary(
        self,
        summary_date: date,
        chat_id: str,
        sent_at: datetime,
        message_id: int | None,
        forecast_run_id: str,
    ) -> None: ...


class Sender(Protocol):
    """Sends text to the chat and returns Telegram's message_id."""

    def send(self, chat_id: str, text: str) -> int | None: ...


def build_summary(store: DataStore, cfg: settings.BotConfig, now_local: datetime) -> str:
    """Collect mart data and render the morning summary."""
    return format_summary(
        store.active_alerts(),
        store.upcoming_events(cfg.horizon_hours),
        store.beach_top(now_local.date(), 3),
        store.wind_series(cfg.surf_spot_id, 13),
        store.surf_windows(cfg.surf_spot_id, 24),
        cfg,
        now_local,
    )


def run_summary(
    store: SummaryStore,
    sender: Sender | None,
    cfg: settings.BotConfig,
    chat_id: str,
    now_utc: datetime,
    dry_run: bool = False,
) -> int:
    """Send today's summary once. Returns the exit code (0 sent / already sent, 1 error)."""
    now_local = now_utc.astimezone(TZ).replace(tzinfo=None)
    day = now_local.date()
    if dry_run:
        print(build_summary(store, cfg, now_local))
        return 0
    if sender is None:
        log.error("no sender configured")
        return 1
    if store.summary_sent(day, chat_id):
        log.info("summary for %s already sent; nothing to do", day)
        return 0
    run_id = store.forecast_run_id()
    if run_id is None:
        log.error("no forecast_run_id in marts; refusing to send summary")
        return 1
    text = build_summary(store, cfg, now_local)
    message_id = sender.send(chat_id, text)
    store.record_summary(day, chat_id, datetime.now(UTC), message_id, run_id)
    log.info("summary for %s sent (message_id=%s)", day, message_id)
    return 0


class TelegramSender:
    """Sender backed by python-telegram-bot."""

    def __init__(self, token: str) -> None:
        self._token = token

    def send(self, chat_id: str, text: str) -> int | None:
        from telegram import Bot
        from telegram.constants import ParseMode

        async def _go() -> int | None:
            async with Bot(self._token) as bot:
                msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
                return int(msg.message_id)

        return asyncio.run(_go())


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Send the morning weather summary once.")
    parser.add_argument("--dry-run", action="store_true", help="print only; no send, no write")
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

        store = BigQueryStore(env)
        now = datetime.now(UTC)
        if args.dry_run:
            code = run_summary(store, None, cfg, env.chat_id or "", now, dry_run=True)
        elif not env.token or not env.chat_id:
            log.error("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required")
        else:
            code = run_summary(store, TelegramSender(env.token), cfg, env.chat_id, now)
    except Exception:
        log.exception("morning summary failed")
        code = 1
    sys.exit(code)
