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
    RATE_LIMIT_TEXT,
    DataStore,
    attribution_footer,
    format_alerts,
    format_beaches,
    format_surf,
    format_wind,
    header_line,
    help_text,
    stale_notice,
    target_beach_day,
    truncate_message,
    with_footer,
    with_notice,
)
from .rain import format_rain, rain_window
from .ratelimit import RateLimiter, Verdict

log = logging.getLogger("alacati_bot")

Builder = Callable[[DataStore, settings.BotConfig, datetime], str]


def now_local() -> datetime:
    return datetime.now(UTC).astimezone(TZ).replace(tzinfo=None)


def _finish(body: str, store: DataStore, cfg: settings.BotConfig, now: datetime) -> str:
    """Prefix header + stale-data warning and fit the Telegram length limit."""
    return truncate_message(_prefix(body, store, cfg, now), cfg)


def _prefix(body: str, store: DataStore, cfg: settings.BotConfig, now: datetime) -> str:
    fetched = store.latest_fetched_at()
    notice = stale_notice(store, cfg, now, fetched)
    return with_notice(body, notice, header_line(now, fetched))


def reply_alerts(store: DataStore, cfg: settings.BotConfig, now: datetime) -> str:
    body = format_alerts(store.active_alerts(), store.upcoming_events(cfg.horizon_hours), cfg, now)
    return _finish(body, store, cfg, now)


def reply_beaches(store: DataStore, cfg: settings.BotConfig, now: datetime) -> str:
    day, tomorrow = target_beach_day(now, cfg)
    body = format_beaches(store.beach_top(day, cfg.params.top_beaches), tomorrow, cfg)
    return _finish(body, store, cfg, now)


def reply_wind(store: DataStore, cfg: settings.BotConfig, now: datetime) -> str:
    spot, p = cfg.surf_spot_id, cfg.params
    series = store.wind_series(spot, p.wind_span_hours + 1)
    body = format_wind(series, store.surf_windows(spot, p.surf_window_hours), cfg, now)
    return _finish(body, store, cfg, now)


def reply_rain(store: DataStore, cfg: settings.BotConfig, now: datetime) -> str:
    start_utc, end_utc = rain_window(now)
    body = format_rain(store.rain_hours(start_utc, end_utc), cfg, now)
    return _finish(body, store, cfg, now)


def reply_surf(store: DataStore, cfg: settings.BotConfig, now: datetime) -> str:
    p = cfg.params
    spots = [
        (
            sid,
            name,
            sport,
            store.wind_series(sid, p.wind_span_hours + 1),
            store.surf_windows(sid, p.surf_window_hours),
        )
        for sid, name, sport in cfg.surf_spots
    ]
    body = format_surf(spots, cfg, now)
    return truncate_message(_prefix(body, store, cfg, now), cfg, attribution_footer(cfg))


def reply_help(store: DataStore, cfg: settings.BotConfig, now: datetime) -> str:
    return help_text(cfg, now)


def _make_handler(
    build: Builder,
    store: DataStore,
    cfg: settings.BotConfig,
    limiter: RateLimiter,
    short_footer: bool = False,
) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Any]:
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        chat = update.effective_chat
        verdict = limiter.check(chat.id if chat else 0)
        if verdict is Verdict.IGNORE:
            return
        wrap = (
            (lambda t: t + attribution_footer(cfg))
            if short_footer
            else (lambda t: with_footer(t, cfg))
        )
        if verdict is Verdict.NOTIFY:
            text = wrap(RATE_LIMIT_TEXT)
        else:
            try:
                # The thread cannot be killed, but every BigQuery call has its own timeout.
                text = await asyncio.wait_for(
                    asyncio.to_thread(build, store, cfg, now_local()),
                    timeout=cfg.params.query_timeout_s,
                )
            except Exception:  # includes TimeoutError
                log.exception("command failed")
                text = wrap(ERROR_TEXT)
        await update.message.reply_text(
            text, parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )

    return handler


def build_application(
    token: str,
    store: DataStore,
    cfg: settings.BotConfig,
    limiter: RateLimiter | None = None,
) -> Application[Any, Any, Any, Any, Any, Any]:
    app = Application.builder().token(token).build()
    p = cfg.params
    lim = limiter or RateLimiter(p.rate_limit_commands, p.rate_limit_window_s)

    app.add_handler(CommandHandler(["start", "yardim"], _make_handler(reply_help, store, cfg, lim)))
    app.add_handler(CommandHandler("uyari", _make_handler(reply_alerts, store, cfg, lim)))
    app.add_handler(CommandHandler("plaj", _make_handler(reply_beaches, store, cfg, lim)))
    app.add_handler(CommandHandler("ruzgar", _make_handler(reply_wind, store, cfg, lim)))
    app.add_handler(CommandHandler(["yagis", "yagmur"], _make_handler(reply_rain, store, cfg, lim)))
    app.add_handler(
        CommandHandler(["sorf", "surf"], _make_handler(reply_surf, store, cfg, lim, True))
    )
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
    app = build_application(env.token, BigQueryStore(env, cfg.params), cfg)
    log.info("bot starting (long polling)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
