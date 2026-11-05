from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest
from conftest import NOW, make_payload
from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from alacati_ingestion.bq_loader import (
    BigQueryStore,
    LoadError,
    OpsWriteError,
    call_log_row,
    flatten_message,
    ingest_run_row,
    job_id,
    table_columns,
)
from alacati_ingestion.messages import build_message
from alacati_ingestion.open_meteo import ApiConfig, CallRecord
from alacati_ingestion.runner import RunSummary
from alacati_ingestion.settings import BigQuerySettings

RUN_ID = "11111111-1111-4111-8111-111111111111"
SETTINGS = BigQuerySettings(
    "proj", "EU", {"raw": "alacati_raw", "ops": "alacati_ops", "staging": "s", "marts": "m"}
)


class FakeJob:
    def __init__(self, state: str = "DONE", error_result: dict[str, str] | None = None) -> None:
        self.state = state
        self.error_result = error_result

    def result(self) -> None:
        if self.error_result is not None:
            raise RuntimeError("job failed")


class FakeClient:
    """Records load_table_from_json calls; optionally raises Conflict for given job ids."""

    def __init__(
        self,
        conflict_on: set[str] | None = None,
        existing: FakeJob | None = None,
        fail_tables: tuple[str, ...] = (),
    ) -> None:
        self.loads: list[dict[str, Any]] = []
        self.conflict_on = conflict_on or set()
        self.existing = existing or FakeJob()
        self.fail_tables = fail_tables

    def get_job(self, jid: str, **kw: Any) -> FakeJob:
        return self.existing

    def load_table_from_json(self, rows: Any, table: str, **kw: Any) -> FakeJob:
        if kw["job_id"] in self.conflict_on:
            raise Conflict("exists")
        if any(t in kw["job_id"] for t in self.fail_tables):
            raise RuntimeError("load boom")
        self.loads.append({"rows": rows, "table": table, **kw})
        return FakeJob()


def _store(client: FakeClient) -> BigQueryStore:
    return BigQueryStore(cast(bigquery.Client, client), SETTINGS)


def _message(config: ApiConfig, source: str) -> dict[str, Any]:
    return build_message(
        run_id=RUN_ID,
        run_expected_messages=2,
        source=source,
        point_id="x",
        fetched_at=NOW,
        latitude=38.0,
        longitude=26.0,
        forecast_days=3,
        past_hours=3,
        model="best_match",
        variables=config.variables(source),
        response=make_payload(config, source),
    )


@pytest.mark.parametrize(
    ("source", "table"), [("forecast", "forecast_hourly"), ("marine", "marine_hourly")]
)
def test_flattened_columns_match_contract(config: ApiConfig, source: str, table: str) -> None:
    rows = flatten_message(_message(config, source), datetime.now(UTC))
    assert len(rows) == 4
    for row in rows:
        assert list(row) == table_columns(table)
    assert rows[0]["request_past_hours"] == 3
    assert rows[0]["schema_version"] == 1
    id_col = "location_id" if source == "forecast" else "marine_zone_id"
    assert rows[0][id_col] == "x"


def test_ops_rows_match_contract() -> None:
    call = CallRecord("c1", NOW, "forecast", "x", 1.0, 200, True, 12, None, 3)
    assert list(call_log_row(call, RUN_ID)) == table_columns("api_call_log")
    summary = RunSummary(RUN_ID, "manual", NOW, 3, 17, NOW, 17, 0, "complete")
    assert list(ingest_run_row(summary)) == table_columns("ingest_runs")


def test_job_id_is_deterministic() -> None:
    assert job_id("raw", "forecast_hourly", RUN_ID, 0) == f"raw_forecast_hourly_{RUN_ID}_0"


def test_load_raw_uses_batch_load_with_deterministic_ids(config: ApiConfig) -> None:
    client = FakeClient()
    stored = _store(client).load_raw(
        RUN_ID, [_message(config, "forecast"), _message(config, "marine")]
    )
    assert len(stored) == 2
    ids = [load["job_id"] for load in client.loads]
    assert ids == [f"raw_forecast_hourly_{RUN_ID}_0", f"raw_marine_hourly_{RUN_ID}_0"]
    first = client.loads[0]
    assert first["table"] == "proj.alacati_raw.forecast_hourly"
    assert first["location"] == "EU"
    assert first["job_config"].write_disposition == "WRITE_APPEND"
    assert first["job_config"].schema


def test_conflict_with_succeeded_job_means_loaded(config: ApiConfig) -> None:
    client = FakeClient(conflict_on={f"raw_forecast_hourly_{RUN_ID}_0"})
    stored = _store(client).load_raw(RUN_ID, [_message(config, "forecast")])
    assert len(stored) == 1
    assert client.loads == []


def test_conflict_with_failed_job_raises(config: ApiConfig) -> None:
    jid = f"raw_forecast_hourly_{RUN_ID}_0"
    client = FakeClient(conflict_on={jid}, existing=FakeJob("DONE", {"reason": "invalid"}))
    store = _store(client)
    with pytest.raises(LoadError):
        store._load("raw", "forecast_hourly", RUN_ID, [{"a": 1}])
    assert store.load_raw(RUN_ID, [_message(config, "forecast")]) == []


def test_conflict_with_job_not_done_raises() -> None:
    jid = f"raw_forecast_hourly_{RUN_ID}_0"
    client = FakeClient(conflict_on={jid}, existing=FakeJob("RUNNING"))
    with pytest.raises(LoadError):
        _store(client)._load("raw", "forecast_hourly", RUN_ID, [{"a": 1}])


def test_load_ops_writes_both_tables() -> None:
    client = FakeClient()
    call = CallRecord("c1", NOW, "forecast", "x", 1.0, 200, True, 12, None, 3)
    summary = RunSummary(RUN_ID, "manual", NOW, 3, 1, NOW, 1, 0, "complete")
    _store(client).load_ops([call], summary)
    assert [load["job_id"] for load in client.loads] == [
        f"ops_ingest_runs_{RUN_ID}_0",
        f"ops_api_call_log_{RUN_ID}_0",
    ]


def test_load_ops_writes_runs_first_and_survives_call_log_failure() -> None:
    client = FakeClient(fail_tables=("api_call_log",))
    call = CallRecord("c1", NOW, "forecast", "x", 1.0, 200, True, 12, None, 3)
    summary = RunSummary(RUN_ID, "manual", NOW, 3, 1, NOW, 1, 0, "complete")
    with pytest.raises(OpsWriteError, match="api_call_log"):
        _store(client).load_ops([call], summary)
    assert [load["job_id"] for load in client.loads] == [f"ops_ingest_runs_{RUN_ID}_0"]


def test_load_ops_runs_failure_still_attempts_call_log() -> None:
    client = FakeClient(fail_tables=("ingest_runs",))
    call = CallRecord("c1", NOW, "forecast", "x", 1.0, 200, True, 12, None, 3)
    summary = RunSummary(RUN_ID, "manual", NOW, 3, 1, NOW, 1, 0, "complete")
    with pytest.raises(OpsWriteError, match="ingest_runs"):
        _store(client).load_ops([call], summary)
    assert [load["job_id"] for load in client.loads] == [f"ops_api_call_log_{RUN_ID}_0"]
