"""BigQuery data access: SELECT-only on marts, parameterised, TTL cache."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from datetime import date, datetime
from typing import Any

from google.cloud import bigquery

from .settings import BotParams, Env

TTL_SECONDS = 600.0  # default for TTLCache only; the store uses data.cache_ttl_minutes
_NOW_HOUR = "TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)"

Row = dict[str, Any]


class TTLCache:
    """Tiny thread-safe in-memory cache."""

    def __init__(
        self, ttl: float = TTL_SECONDS, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._ttl = ttl
        self._clock = clock
        self._data: dict[Any, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get_or_set(self, key: Any, fn: Callable[[], Any]) -> Any:
        now = self._clock()
        with self._lock:
            hit = self._data.get(key)
            if hit and now - hit[0] < self._ttl:
                return hit[1]
        value = fn()
        with self._lock:
            self._data[key] = (self._clock(), value)
        return value


class BigQueryStore:
    """Mart reader plus summary_log access."""

    def __init__(self, env: Env, params: BotParams, client: bigquery.Client | None = None) -> None:
        self._env = env
        self._timeout = params.query_timeout_s
        self._client = client or bigquery.Client(project=env.project, location=env.bq_location)
        self._cache = TTLCache(ttl=params.cache_ttl_minutes * 60)

    def _marts(self, table: str) -> str:
        return f"`{self._client.project}.{self._env.marts_dataset}.{table}`"

    def _ops(self, table: str) -> str:
        return f"`{self._client.project}.{self._env.ops_dataset}.{table}`"

    def _run(self, sql: str, params: list[Any] | None = None) -> list[Row]:
        cfg = bigquery.QueryJobConfig(query_parameters=params or [])
        return [
            dict(r) for r in self._client.query(sql, job_config=cfg).result(timeout=self._timeout)
        ]

    def _cached(self, key: tuple[Any, ...], sql: str, params: list[Any] | None = None) -> list[Row]:
        return list(self._cache.get_or_set(key, lambda: self._run(sql, params)))

    def active_alerts(self) -> list[Row]:
        sql = (
            "SELECT event_id, hazard, level, start_local, end_local, peak_value, peak_unit, "
            "affected_regions "
            f"FROM {self._marts('mart_active_alerts')} WHERE status = 'active' ORDER BY start_utc"
        )
        return self._cached(("active",), sql)

    def upcoming_events(self, horizon_hours: int) -> list[Row]:
        sql = (
            "SELECT episode_key, hazard, level, start_utc, end_utc, peak_value, peak_unit, "
            "affected_regions "
            f"FROM {self._marts('mart_alert_events')} "
            "WHERE end_utc > CURRENT_TIMESTAMP() "
            "AND start_utc < TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @h HOUR) "
            "ORDER BY start_utc"
        )
        params = [bigquery.ScalarQueryParameter("h", "INT64", horizon_hours)]
        return self._cached(("events", horizon_hours), sql, params)

    def beach_top(self, day: date, limit: int) -> list[Row]:
        sql = (
            "SELECT location_name, avg_score, best_hour_local, rank_in_day, label_tr, "
            "shore_verified "
            f"FROM {self._marts('mart_beach_scores_daily')} "
            "WHERE date_local = @d AND rank_in_day <= @n ORDER BY rank_in_day"
        )
        params = [
            bigquery.ScalarQueryParameter("d", "DATE", day),
            bigquery.ScalarQueryParameter("n", "INT64", limit),
        ]
        return self._cached(("beach", day, limit), sql, params)

    def wind_series(self, location_id: str, hours: int) -> list[Row]:
        sql = (
            "SELECT time_local, wind_speed_10m, wind_gusts_10m, wind_direction_10m "
            f"FROM {self._marts('mart_location_weather_hourly')} "
            "WHERE location_id = @loc "
            f"AND time_utc >= {_NOW_HOUR} "
            f"AND time_utc < TIMESTAMP_ADD({_NOW_HOUR}, INTERVAL @h HOUR) "
            "AND wind_speed_10m IS NOT NULL ORDER BY time_utc"
        )
        params = [
            bigquery.ScalarQueryParameter("loc", "STRING", location_id),
            bigquery.ScalarQueryParameter("h", "INT64", hours),
        ]
        return self._cached(("wind", location_id, hours), sql, params)

    def surf_windows(self, spot_id: str, hours: int) -> list[Row]:
        sql = (
            "SELECT spot_id, sport, quality, window_start_utc, window_end_utc, avg_wind_kmh, "
            "max_gust_kmh "
            f"FROM {self._marts('mart_surf_windows')} "
            "WHERE spot_id = @spot AND window_end_utc > CURRENT_TIMESTAMP() "
            "AND window_start_utc < TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @h HOUR) "
            "ORDER BY window_start_utc"
        )
        params = [
            bigquery.ScalarQueryParameter("spot", "STRING", spot_id),
            bigquery.ScalarQueryParameter("h", "INT64", hours),
        ]
        return self._cached(("windows", spot_id, hours), sql, params)

    def forecast_run_id(self) -> str | None:
        sql = (
            "SELECT forecast_run_id "
            f"FROM {self._marts('mart_location_weather_hourly')} "
            f"WHERE time_utc >= TIMESTAMP_SUB({_NOW_HOUR}, INTERVAL 24 HOUR) "
            "AND forecast_run_id IS NOT NULL "
            "ORDER BY forecast_fetched_at DESC LIMIT 1"
        )
        rows = self._cached(("run",), sql)
        return str(rows[0]["forecast_run_id"]) if rows else None

    def latest_fetched_at(self) -> datetime | None:
        sql = (
            "SELECT MAX(forecast_fetched_at) AS t "
            f"FROM {self._marts('mart_location_weather_hourly')} "
            f"WHERE time_utc >= TIMESTAMP_SUB({_NOW_HOUR}, INTERVAL 24 HOUR)"
        )
        rows = self._cached(("fetched",), sql)
        return rows[0]["t"] if rows and rows[0]["t"] is not None else None

    # --- ops.summary_log (never cached) ---------------------------------------------

    def summary_sent(self, summary_date: date, chat_id: str) -> bool:
        sql = (
            "SELECT COUNT(*) AS n "
            f"FROM {self._ops('summary_log')} "
            "WHERE summary_date = @d AND chat_id = @c"
        )
        params = [
            bigquery.ScalarQueryParameter("d", "DATE", summary_date),
            bigquery.ScalarQueryParameter("c", "STRING", chat_id),
        ]
        return int(self._run(sql, params)[0]["n"]) > 0

    def record_summary(
        self,
        summary_date: date,
        chat_id: str,
        sent_at: datetime,
        message_id: int | None,
        forecast_run_id: str,
    ) -> None:
        sql = (
            f"MERGE {self._ops('summary_log')} T "
            "USING (SELECT @d AS summary_date, @c AS chat_id, @ts AS sent_at, "
            "@mid AS telegram_message_id, @run AS forecast_run_id) S "
            "ON T.summary_date = S.summary_date AND T.chat_id = S.chat_id "
            "WHEN NOT MATCHED THEN INSERT "
            "(summary_date, chat_id, sent_at, telegram_message_id, forecast_run_id) "
            "VALUES (S.summary_date, S.chat_id, S.sent_at, S.telegram_message_id, "
            "S.forecast_run_id)"
        )
        params = [
            bigquery.ScalarQueryParameter("d", "DATE", summary_date),
            bigquery.ScalarQueryParameter("c", "STRING", chat_id),
            bigquery.ScalarQueryParameter("ts", "TIMESTAMP", sent_at),
            bigquery.ScalarQueryParameter("mid", "INT64", message_id),
            bigquery.ScalarQueryParameter("run", "STRING", forecast_run_id),
        ]
        self._run(sql, params)

    # --- ops.alert_deliveries (never cached) ----------------------------------------

    def alert_delivered(self, notification_id: str, chat_id: str) -> bool:
        sql = (
            "SELECT COUNT(*) AS n "
            f"FROM {self._ops('alert_deliveries')} "
            "WHERE notification_id = @n AND chat_id = @c"
        )
        params = [
            bigquery.ScalarQueryParameter("n", "STRING", notification_id),
            bigquery.ScalarQueryParameter("c", "STRING", chat_id),
        ]
        return int(self._run(sql, params)[0]["n"]) > 0

    def record_alert_delivery(
        self, notification_id: str, chat_id: str, sent_at: datetime, message_id: int | None
    ) -> None:
        sql = (
            f"MERGE {self._ops('alert_deliveries')} T "
            "USING (SELECT @n AS notification_id, @c AS chat_id, @ts AS sent_at, "
            "@mid AS telegram_message_id) S "
            "ON T.notification_id = S.notification_id AND T.chat_id = S.chat_id "
            "WHEN NOT MATCHED THEN INSERT "
            "(notification_id, chat_id, sent_at, telegram_message_id) "
            "VALUES (S.notification_id, S.chat_id, S.sent_at, S.telegram_message_id)"
        )
        params = [
            bigquery.ScalarQueryParameter("n", "STRING", notification_id),
            bigquery.ScalarQueryParameter("c", "STRING", chat_id),
            bigquery.ScalarQueryParameter("ts", "TIMESTAMP", sent_at),
            bigquery.ScalarQueryParameter("mid", "INT64", message_id),
        ]
        self._run(sql, params)
