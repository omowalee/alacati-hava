"""Flatten weather.raw messages and load them into BigQuery with batch load jobs.

No streaming inserts. Job ids are deterministic ('raw_<table>_<run_id>_<n>'), so a retried
load of the same batch is rejected by BigQuery (Conflict) and treated as already loaded.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from alacati_ingestion.bq_setup import contract_schema, load_table_contracts
from alacati_ingestion.open_meteo import CallRecord
from alacati_ingestion.runner import RunSummary
from alacati_ingestion.settings import BigQuerySettings

log = logging.getLogger(__name__)

MAX_ROWS_PER_JOB = 50_000
RAW_TABLE_FOR_SOURCE = {"forecast": "forecast_hourly", "marine": "marine_hourly"}
POINT_COLUMN_FOR_SOURCE = {"forecast": "location_id", "marine": "marine_zone_id"}


def iso(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def job_id(prefix: str, table: str, run_id: str, n: int) -> str:
    """Deterministic load job id, e.g. raw_forecast_hourly_<run_id>_0."""
    return f"{prefix}_{table}_{run_id}_{n}"


def table_columns(table: str) -> list[str]:
    """Column names of a table, in contract order."""
    contract = next(t for t in load_table_contracts() if t["name"] == table)
    return [f.name for f in contract_schema(contract)]


def flatten_message(message: dict[str, Any], ingested_at: datetime) -> list[dict[str, Any]]:
    """One row per hour; columns match contracts/bigquery/raw/<table>.json exactly."""
    source = message["source"]
    columns = table_columns(RAW_TABLE_FOR_SOURCE[source])
    lineage = {
        POINT_COLUMN_FOR_SOURCE[source]: message["point_id"],
        "message_id": message["message_id"],
        "run_id": message["run_id"],
        "fetched_at": message["fetched_at"],
        "request_latitude": message["request"]["latitude"],
        "request_longitude": message["request"]["longitude"],
        "request_past_hours": message["request"]["past_hours"],
        "request_forecast_days": message["request"]["forecast_days"],
        "grid_latitude": message["grid"]["latitude"],
        "grid_longitude": message["grid"]["longitude"],
        "grid_elevation_m": message["grid"]["elevation_m"],
        "model": message["model"],
        "schema_version": message["schema_version"],
        "ingested_at": iso(ingested_at),
    }
    rows = []
    for hour in message["hourly"]:
        values = {**lineage, **hour}  # hour carries time_utc plus the weather variables
        rows.append({col: values.get(col) for col in columns})
    return rows


def call_log_row(call: CallRecord, run_id: str) -> dict[str, Any]:
    return {
        "call_id": call.call_id,
        "run_id": run_id,
        "called_at": iso(call.called_at),
        "api": call.api,
        "point_id": call.point_id,
        "weighted_calls": call.weighted_calls,
        "http_status": call.http_status,
        "success": call.success,
        "duration_ms": call.duration_ms,
        "error": call.error,
        "past_hours": call.past_hours,
    }


def ingest_run_row(summary: RunSummary) -> dict[str, Any]:
    return {
        "run_id": summary.run_id,
        "trigger": summary.trigger,
        "started_at": iso(summary.started_at),
        "finished_at": iso(summary.finished_at) if summary.finished_at else None,
        "past_hours": summary.past_hours,
        "expected_messages": summary.expected_messages,
        "succeeded_messages": summary.succeeded_messages,
        "failed_messages": summary.failed_messages,
        "status": summary.status,
    }


class BigQueryStore:
    """RunStore backed by BigQuery (reads ops tables, batch-loads raw and ops rows)."""

    def __init__(self, client: bigquery.Client, settings: BigQuerySettings) -> None:
        self._client = client
        self._settings = settings
        self._contracts = {t["name"]: t for t in load_table_contracts()}

    def _table_ref(self, table: str) -> str:
        dataset = self._settings.datasets[self._contracts[table]["dataset"]]
        return f"{self._settings.project_id}.{dataset}.{table}"

    # --- reads -----------------------------------------------------------------
    def todays_weighted_calls(self, now: datetime) -> float:
        day_start = now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        sql = (
            "SELECT COALESCE(SUM(weighted_calls), 0) AS total "
            f"FROM `{self._table_ref('api_call_log')}` WHERE called_at >= @day_start"
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("day_start", "TIMESTAMP", day_start)]
        )
        rows = list(self._client.query(sql, job_config=job_config).result())
        return float(rows[0]["total"])

    def last_complete_run_started_at(self) -> datetime | None:
        sql = (
            "SELECT MAX(started_at) AS started_at "
            f"FROM `{self._table_ref('ingest_runs')}` WHERE status = 'complete'"
        )
        rows = list(self._client.query(sql).result())
        value: datetime | None = rows[0]["started_at"] if rows else None
        return value

    # --- writes ----------------------------------------------------------------
    def _load(self, prefix: str, table: str, run_id: str, rows: list[dict[str, Any]]) -> None:
        schema = contract_schema(self._contracts[table])
        for n, start in enumerate(range(0, len(rows), MAX_ROWS_PER_JOB)):
            chunk = rows[start : start + MAX_ROWS_PER_JOB]
            jid = job_id(prefix, table, run_id, n)
            config = bigquery.LoadJobConfig(
                schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
            try:
                job = self._client.load_table_from_json(
                    chunk,
                    self._table_ref(table),
                    job_id=jid,
                    job_config=config,
                    location=self._settings.location,
                )
            except Conflict:
                log.warning("load job %s already exists: treating as already loaded", jid)
                continue
            job.result()
            log.info("loaded %d rows into %s (job %s)", len(chunk), table, jid)

    def load_raw(self, run_id: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ingested_at = datetime.now(UTC)
        stored: list[dict[str, Any]] = []
        for source, table in RAW_TABLE_FOR_SOURCE.items():
            batch = [m for m in messages if m["source"] == source]
            if not batch:
                continue
            rows = [r for m in batch for r in flatten_message(m, ingested_at)]
            try:
                self._load("raw", table, run_id, rows)
            except Exception:
                log.exception("raw load into %s failed; its messages count as failed", table)
                continue
            stored.extend(batch)
        return stored

    def load_ops(self, calls: list[CallRecord], summary: RunSummary) -> None:
        if calls:
            rows = [call_log_row(c, summary.run_id) for c in calls]
            self._load("ops", "api_call_log", summary.run_id, rows)
        self._load("ops", "ingest_runs", summary.run_id, [ingest_run_row(summary)])
