"""Long-polling Telegram bot with /uyari, /plaj, /ruzgar commands."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

from . import settings
from .formatting import TZ
from .messages import (
    ERROR_TEXT,
    DataStore,
    format_alerts,
    format_beaches,
    format_wind,
    help_text,
    target_beach_day,
    with_footer,
)

log = logging.getLogger("alacati_bot")

Builder = Callable[[DataStore, settings.BotConfig, datetime], str]


def now_local() -> datetime:
    return datetime.now(UTC).astimezone(TZ).replace(tzinfo=None)


def reply_alerts(store: DataStore, cfg: settings.BotConfig, now: datetime) -> str:
    return format_alerts(store.active_alerts(), store.upcoming_events(cfg.horizon_hours), cfg, now)


def reply_beaches(store: DataStore, cfg: settings.BotConfig, now: datetime) -> str:
    day, tomorrow = target_beach_day(now)
    return format_beaches(store.beach_top(day, 5), tomorrow, cfg)


def reply_wind(store: DataStore, cfg: settings.BotConfig, now: datetime) -> str:
    spot = cfg.surf_spot_id
    return format_wind(store.wind_series(spot, 13), store.surf_windows(spot, 24), cfg, now)


def _make_handler(
    build: Builder, store: DataStore, cfg: settings.BotConfig
) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Any]:
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        try:
            text = await asyncio.to_thread(build, store, cfg, now_local())
        except Exception:
            log.exception("command failed")
            text = with_footer(ERROR_TEXT, cfg)
        await update.message.reply_text(
            text, parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )

    return handler


def build_application(
    token: str, store: DataStore, cfg: settings.BotConfig
) -> Application[Any, Any, Any, Any, Any, Any]:
    app = Application.builder().token(token).build()

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is not None:
            await update.message.reply_text(help_text(cfg), parse_mode=ParseMode.HTML)

    app.add_handler(CommandHandler(["start", "yardim"], start))
    app.add_handler(CommandHandler("uyari", _make_handler(reply_alerts, store, cfg)))
    app.add_handler(CommandHandler("plaj", _make_handler(reply_beaches, store, cfg)))
    app.add_handler(CommandHandler("ruzgar", _make_handler(reply_wind, store, cfg)))
    return app


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    # httpx logs full request URLs (which contain the bot token) at INFO.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    settings.load_env()
    env = settings.load_settings()
    if not env.token:
        raise SystemExit("TELEGRAM_BOT_TOKEN is not set")
    from .store import BigQueryStore

    cfg = settings.load_config()
    app = build_application(env.token, BigQueryStore(env), cfg)
    log.info("bot starting (long polling)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
