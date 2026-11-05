"""BigQuery access: SELECT-only, explicit columns, tiny cached queries on marts."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

import settings

TTL = 600
_SCOPES = ["https://www.googleapis.com/auth/bigquery"]
_NOW_HOUR = "TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)"


def _service_account_info() -> dict[str, Any] | None:
    try:
        if "gcp_service_account" in st.secrets:
            return dict(st.secrets["gcp_service_account"])
    except Exception:  # no secrets.toml
        pass
    return None


@st.cache_resource
def get_client() -> bigquery.Client:
    """BigQuery client: service account from st.secrets if present, else ADC."""
    settings.load_env()
    project = settings.secret_or_env(st.secrets, "GCP_PROJECT_ID")
    sa = _service_account_info()
    if sa:
        creds = service_account.Credentials.from_service_account_info(sa, scopes=_SCOPES)
        return bigquery.Client(
            project=project or sa.get("project_id"),
            credentials=creds,
            location=settings.BQ_LOCATION,
        )
    return bigquery.Client(project=project, location=settings.BQ_LOCATION)


def _table(name: str) -> str:
    project = get_client().project
    dataset = settings.secret_or_env(st.secrets, "BQ_DATASET_MARTS", settings.DEFAULT_DATASET)
    return f"`{project}.{dataset}.{name}`"


def _run(sql: str, params: list[Any] | None = None) -> pd.DataFrame:
    cfg = bigquery.QueryJobConfig(query_parameters=params or [])
    return get_client().query(sql, job_config=cfg).to_dataframe(create_bqstorage_client=False)


@st.cache_data(ttl=TTL)
def last_update() -> datetime | None:
    """Latest forecast fetch time (UTC, naive) over the weather mart."""
    df = _run(
        f"SELECT MAX(forecast_fetched_at) AS ts FROM {_table('mart_location_weather_hourly')} "
        f"WHERE time_utc >= TIMESTAMP_SUB({_NOW_HOUR}, INTERVAL 24 HOUR)"
    )
    ts = df["ts"].iloc[0] if len(df) else None
    return None if pd.isna(ts) else pd.Timestamp(ts).to_pydatetime()


@st.cache_data(ttl=TTL)
def active_alerts() -> pd.DataFrame:
    """Currently active alerts (filled from Phase 2 on)."""
    return _run(
        "SELECT event_id, hazard, hazard_label_tr, level, status, start_local, end_local, "
        "peak_value, peak_unit, peak_time_local, affected_regions "
        f"FROM {_table('mart_active_alerts')} WHERE status = 'active' ORDER BY start_utc"
    )


@st.cache_data(ttl=TTL)
def alert_events(horizon_hours: int) -> pd.DataFrame:
    """Episodes overlapping [now, now + horizon]; local times are derived in the UI."""
    return _run(
        "SELECT episode_key, hazard, level, start_utc, end_utc, peak_value, peak_unit, "
        "affected_regions "
        f"FROM {_table('mart_alert_events')} "
        "WHERE end_utc > CURRENT_TIMESTAMP() "
        "AND start_utc < TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @h HOUR) "
        "ORDER BY start_utc",
        [bigquery.ScalarQueryParameter("h", "INT64", horizon_hours)],
    )


@st.cache_data(ttl=TTL)
def weather_now() -> pd.DataFrame:
    """Current-hour weather for every location (map tooltips for surf spots)."""
    return _run(
        "SELECT location_id, location_name, location_type, time_local, wind_speed_10m, "
        "wind_gusts_10m, wind_direction_10m, wave_height "
        f"FROM {_table('mart_location_weather_hourly')} WHERE time_utc = {_NOW_HOUR}"
    )


@st.cache_data(ttl=TTL)
def weather_series(location_id: str, hours: int) -> pd.DataFrame:
    """Hourly series for one location: a few past hours plus the next ``hours``."""
    return _run(
        "SELECT time_local, temperature_2m, precipitation, wind_speed_10m, wind_gusts_10m, "
        "wind_direction_10m, wave_height "
        f"FROM {_table('mart_location_weather_hourly')} "
        "WHERE location_id = @loc "
        f"AND time_utc >= TIMESTAMP_SUB({_NOW_HOUR}, INTERVAL 6 HOUR) "
        f"AND time_utc < TIMESTAMP_ADD({_NOW_HOUR}, INTERVAL @h HOUR) "
        "ORDER BY time_local",
        [
            bigquery.ScalarQueryParameter("loc", "STRING", location_id),
            bigquery.ScalarQueryParameter("h", "INT64", hours),
        ],
    )


@st.cache_data(ttl=TTL)
def beach_scores_hourly(hours: int) -> pd.DataFrame:
    """Beach scores from the current hour up to ``hours`` ahead."""
    return _run(
        "SELECT location_id, location_name, region_id, time_local, score, label_tr, "
        "rank_in_hour, wind_speed_10m, wind_gusts_10m, wind_direction_10m, wave_height, "
        "shore_verified "
        f"FROM {_table('mart_beach_scores_hourly')} "
        f"WHERE time_utc >= {_NOW_HOUR} "
        f"AND time_utc < TIMESTAMP_ADD({_NOW_HOUR}, INTERVAL @h HOUR) "
        "ORDER BY time_local, rank_in_hour",
        [bigquery.ScalarQueryParameter("h", "INT64", hours)],
    )


@st.cache_data(ttl=TTL)
def beach_scores_daily() -> pd.DataFrame:
    """Daily ranking for today and tomorrow (Europe/Istanbul dates)."""
    return _run(
        "SELECT date_local, location_id, location_name, region_id, avg_score, "
        "best_hour_local, rank_in_day, label_tr, shore_verified "
        f"FROM {_table('mart_beach_scores_daily')} "
        'WHERE date_local BETWEEN CURRENT_DATE("Europe/Istanbul") '
        'AND DATE_ADD(CURRENT_DATE("Europe/Istanbul"), INTERVAL 1 DAY) '
        "ORDER BY date_local, rank_in_day"
    )


@st.cache_data(ttl=TTL)
def surf_windows() -> pd.DataFrame:
    """Upcoming surf/kite windows."""
    return _run(
        "SELECT spot_id, sport, quality, window_start_utc, window_end_utc, hours, "
        "avg_wind_kmh, max_gust_kmh, mean_direction_deg, directions_verified "
        f"FROM {_table('mart_surf_windows')} WHERE window_end_utc > CURRENT_TIMESTAMP() "
        "ORDER BY spot_id, window_start_utc"
    )


@st.cache_data(ttl=TTL)
def precip_daily(day: date) -> pd.DataFrame:
    """Total precipitation (mm) per location for one Europe/Istanbul calendar day."""
    return _run(
        "SELECT location_id, SUM(precipitation) AS precip_mm "
        f"FROM {_table('mart_location_weather_hourly')} "
        "WHERE DATE(time_local) = @d AND time_utc >= TIMESTAMP_SUB(@d_ts, INTERVAL 1 DAY) "
        "AND time_utc < TIMESTAMP_ADD(@d_ts, INTERVAL 2 DAY) "
        "GROUP BY location_id",
        [
            bigquery.ScalarQueryParameter("d", "DATE", day),
            bigquery.ScalarQueryParameter(
                "d_ts", "TIMESTAMP", datetime(day.year, day.month, day.day, tzinfo=UTC)
            ),
        ],
    )


@st.cache_data(ttl=TTL)
def wind_hourly(day: date) -> pd.DataFrame:
    """Hourly wind per location for one Europe/Istanbul calendar day (map arrows)."""
    return _run(
        "SELECT location_id, time_local, wind_speed_10m, wind_gusts_10m, wind_direction_10m "
        f"FROM {_table('mart_location_weather_hourly')} "
        "WHERE DATE(time_local) = @d AND time_utc >= TIMESTAMP_SUB(@d_ts, INTERVAL 1 DAY) "
        "AND time_utc < TIMESTAMP_ADD(@d_ts, INTERVAL 2 DAY) "
        "ORDER BY location_id, time_local",
        [
            bigquery.ScalarQueryParameter("d", "DATE", day),
            bigquery.ScalarQueryParameter(
                "d_ts", "TIMESTAMP", datetime(day.year, day.month, day.day, tzinfo=UTC)
            ),
        ],
    )
