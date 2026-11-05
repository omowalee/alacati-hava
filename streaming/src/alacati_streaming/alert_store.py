"""BigQuery reads and MERGE writes of the alert engine.

Writes go through DML MERGE on the contract's unique key (no streaming inserts); rows are
passed as one JSON query parameter and unpacked in SQL, with the SELECT list generated
from the contract schema, so a column can never be silently dropped or reordered.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from alacati_ingestion.bq_setup import contract_schema, load_table_contracts
from alacati_ingestion.settings import BigQuerySettings
from google.cloud import bigquery

from alacati_streaming.lifecycle import AlertState, Episode, Notification, RegionLevel

log = logging.getLogger(__name__)

# ingest_runs is partitioned by started_at: every query on it is limited to this window.
RUN_LOOKBACK_DAYS = 30
COMPLETE_RUNS_LIMIT = 200


@dataclass(frozen=True)
class RunInfo:
    run_id: str
    status: str
    started_at: datetime
    finished_at: datetime | None


@dataclass(frozen=True)
class MartRun:
    """The run mart_location_weather_hourly was built from (latest complete run)."""

    run: RunInfo
    as_of: datetime  # max forecast_fetched_at of that run


# --- JSON -> typed SQL, generated from the contract -------------------------------------------
_SCALAR = {
    "STRING": "{v}",
    "INT64": "CAST({v} AS INT64)",
    "INTEGER": "CAST({v} AS INT64)",
    "FLOAT64": "CAST({v} AS FLOAT64)",
    "FLOAT": "CAST({v} AS FLOAT64)",
    "TIMESTAMP": "TIMESTAMP({v})",
    "BOOL": "CAST({v} AS BOOL)",
    "BOOLEAN": "CAST({v} AS BOOL)",
}


def _field_expr(field: bigquery.SchemaField, src: str, depth: int = 0) -> str:
    path = f"'$.{field.name}'"
    ftype = field.field_type.upper()
    if field.mode == "REPEATED":
        x, o = f"x{depth}", f"o{depth}"
        if ftype in ("RECORD", "STRUCT"):
            inner = ", ".join(
                f"{_field_expr(sub, x, depth + 1)} AS {sub.name}" for sub in field.fields
            )
            select = f"SELECT AS STRUCT {inner}"
        else:
            select = "SELECT " + _SCALAR[ftype].format(v=f"JSON_VALUE({x})")
        return (
            f"ARRAY({select} FROM UNNEST(JSON_QUERY_ARRAY({src}, {path})) AS {x} "
            f"WITH OFFSET AS {o} ORDER BY {o})"
        )
    return _SCALAR[ftype].format(v=f"JSON_VALUE({src}, {path})")


def json_rows_select(schema: Sequence[bigquery.SchemaField]) -> str:
    """SELECT that turns @payload (a JSON array of row objects) into typed rows."""
    cols = ",\n  ".join(f"{_field_expr(f, 'j')} AS {f.name}" for f in schema)
    return f"SELECT\n  {cols}\nFROM UNNEST(JSON_QUERY_ARRAY(@payload)) AS j"


def merge_sql(target: str, schema: Sequence[bigquery.SchemaField], key: str, update: bool) -> str:
    names = [f.name for f in schema]
    sql = (
        f"MERGE `{target}` AS t\nUSING (\n{json_rows_select(schema)}\n) AS s\n"
        f"ON t.{key} = s.{key}\n"
    )
    if update:
        sets = ", ".join(f"{n} = s.{n}" for n in names if n != key)
        sql += f"WHEN MATCHED THEN UPDATE SET {sets}\n"
    sql += (
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(names)}) "
        f"VALUES ({', '.join('s.' + n for n in names)})"
    )
    return sql


def _bq_ts(ts: datetime | None) -> str | None:
    return None if ts is None else ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")


def _to_json_row(obj: AlertState | Notification) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for name, value in asdict(obj).items():
        if isinstance(value, datetime):
            row[name] = _bq_ts(value)
        elif isinstance(value, tuple):
            row[name] = list(value)  # asdict already turned RegionLevel into dicts
        else:
            row[name] = value
    return row


def _region_levels(raw: Any) -> tuple[RegionLevel, ...]:
    return tuple(RegionLevel(str(r["region_id"]), str(r["level"])) for r in raw or [])


def _strs(raw: Any) -> tuple[str, ...]:
    return tuple(str(v) for v in raw or [])


def episode_from_row(row: Any) -> Episode:
    return Episode(
        episode_key=row["episode_key"],
        hazard=row["hazard"],
        level=row["level"],
        start_utc=row["start_utc"],
        end_utc=row["end_utc"],
        peak_value=float(row["peak_value"]),
        peak_unit=row["peak_unit"],
        peak_time_utc=row["peak_time_utc"],
        affected_regions=_strs(row["affected_regions"]),
        region_levels=_region_levels(row["region_levels"]),
        affected_locations=_strs(row["affected_locations"]),
        forecast_run_id=row["forecast_run_id"],
    )


def state_from_row(row: Any) -> AlertState:
    return AlertState(
        event_id=row["event_id"],
        hazard=row["hazard"],
        status=row["status"],
        level=row["level"],
        start_utc=row["start_utc"],
        end_utc=row["end_utc"],
        peak_value=float(row["peak_value"]),
        peak_unit=row["peak_unit"],
        peak_time_utc=row["peak_time_utc"],
        affected_regions=_strs(row["affected_regions"]),
        region_levels=_region_levels(row["region_levels"]),
        affected_locations=_strs(row["affected_locations"]),
        revision=int(row["revision"]),
        notified_level=row["notified_level"],
        notified_start_utc=row["notified_start_utc"],
        notified_end_utc=row["notified_end_utc"],
        notified_regions=_strs(row["notified_regions"]),
        last_notified_status=row["last_notified_status"],
        last_notified_at=row["last_notified_at"],
        first_detected_at=row["first_detected_at"],
        last_updated_at=row["last_updated_at"],
        last_seen_run_id=row["last_seen_run_id"],
        absent_runs=int(row["absent_runs"]),
        ended_at=row["ended_at"],
    )


class AlertStore:
    def __init__(self, client: bigquery.Client, settings: BigQuerySettings) -> None:
        self._client = client
        self._settings = settings
        self._contracts = {t["name"]: t for t in load_table_contracts()}

    def ref(self, table: str) -> str:
        dataset = self._settings.datasets[self._contracts[table]["dataset"]]
        return f"{self._settings.project_id}.{dataset}.{table}"

    def _query(self, sql: str, params: Sequence[Any] = ()) -> list[Any]:
        config = bigquery.QueryJobConfig(query_parameters=list(params))
        job = self._client.query(sql, job_config=config, location=self._settings.location)
        return list(job.result())

    # --- reads ---------------------------------------------------------------------------
    def run_info(self, run_id: str) -> RunInfo | None:
        rows = self._query(
            "SELECT run_id, status, started_at, finished_at "
            f"FROM `{self.ref('ingest_runs')}` "
            "WHERE run_id = @run_id "
            "AND started_at >= "
            f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {RUN_LOOKBACK_DAYS} DAY) "
            "ORDER BY finished_at DESC LIMIT 1",
            [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)],
        )
        if not rows:
            return None
        r = rows[0]
        return RunInfo(r["run_id"], r["status"], r["started_at"], r["finished_at"])

    def mart_run(self) -> MartRun | None:
        # Past hours in the mart can come from older runs (latest run wins per hour), so
        # the mart's run is the one of the newest forecast_fetched_at.
        rows = self._query(
            "WITH m AS (SELECT ARRAY_AGG(forecast_run_id ORDER BY forecast_fetched_at DESC "
            "LIMIT 1)[SAFE_OFFSET(0)] AS run_id, MAX(forecast_fetched_at) AS as_of "
            f"FROM `{self.ref('mart_location_weather_hourly')}`) "
            "SELECT m.run_id, m.as_of, r.status, r.started_at, r.finished_at FROM m "
            f"LEFT JOIN (SELECT * FROM `{self.ref('ingest_runs')}` WHERE started_at >= "
            f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {RUN_LOOKBACK_DAYS} DAY)) AS r "
            "ON r.run_id = m.run_id"
        )
        if not rows or rows[0]["run_id"] is None or rows[0]["started_at"] is None:
            return None
        r = rows[0]
        return MartRun(
            RunInfo(r["run_id"], r["status"], r["started_at"], r["finished_at"]), r["as_of"]
        )

    def table_modified(self, table: str) -> datetime | None:
        """Last modification time (table metadata: free, no query)."""
        modified: datetime | None = self._client.get_table(self.ref(table)).modified
        return modified

    def episodes(self) -> list[Episode]:
        rows = self._query(f"SELECT * FROM `{self.ref('mart_alert_events')}`")
        return [episode_from_row(r) for r in rows]

    def active_states(self) -> list[AlertState]:
        rows = self._query(f"SELECT * FROM `{self.ref('alert_state')}` WHERE status = 'active'")
        return [state_from_row(r) for r in rows]

    def complete_runs(self, up_to: datetime) -> list[str]:
        """Complete run ids with started_at <= up_to, oldest first (bounded window)."""
        rows = self._query(
            "SELECT run_id FROM ("
            f"SELECT run_id, started_at FROM `{self.ref('ingest_runs')}` "
            "WHERE status = 'complete' AND started_at <= @up_to "
            f"AND started_at >= TIMESTAMP_SUB(@up_to, INTERVAL {RUN_LOOKBACK_DAYS} DAY) "
            f"ORDER BY started_at DESC, run_id DESC LIMIT {COMPLETE_RUNS_LIMIT}"
            ") ORDER BY started_at, run_id",
            [bigquery.ScalarQueryParameter("up_to", "TIMESTAMP", up_to)],
        )
        return [r["run_id"] for r in rows]

    # --- writes --------------------------------------------------------------------------
    def _merge(self, table: str, key: str, objs: Sequence[Any], update: bool) -> None:
        if not objs:
            return
        schema = contract_schema(self._contracts[table])
        payload = json.dumps([_to_json_row(o) for o in objs], ensure_ascii=False)
        self._query(
            merge_sql(self.ref(table), schema, key, update),
            [bigquery.ScalarQueryParameter("payload", "STRING", payload)],
        )
        log.info("merged %d row(s) into %s", len(objs), table)

    def merge_notifications(self, notes: Sequence[Notification]) -> None:
        """Append-only audit log: insert unknown notification_ids, never update."""
        self._merge("alert_notifications", "notification_id", notes, update=False)

    def merge_states(self, states: Sequence[AlertState]) -> None:
        self._merge("alert_state", "event_id", states, update=True)
