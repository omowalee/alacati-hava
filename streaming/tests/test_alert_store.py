"""The engine's rows line up with the BigQuery contracts; MERGE SQL is generated from them."""

from __future__ import annotations

import json
from dataclasses import fields
from datetime import UTC, datetime

from alacati_ingestion.bq_setup import contract_schema, load_table_contracts
from alacati_ingestion.settings import CONTRACTS_DIR
from test_lifecycle import first_event

from alacati_streaming.alert_store import _to_json_row, merge_sql
from alacati_streaming.lifecycle import AlertState, Episode, Notification


def columns(table: str) -> list[str]:
    contract = next(t for t in load_table_contracts() if t["name"] == table)
    return [f.name for f in contract_schema(contract)]


def test_dataclasses_match_the_contract_columns() -> None:
    assert {f.name for f in fields(AlertState)} == set(columns("alert_state"))
    assert {f.name for f in fields(Notification)} == set(columns("alert_notifications"))
    assert {f.name for f in fields(Episode)} <= set(columns("mart_alert_events"))


def test_alert_message_fields_match_the_kafka_contract() -> None:
    schema = json.loads(
        (CONTRACTS_DIR / "kafka" / "weather_alert.v1.schema.json").read_text(encoding="utf-8")
    )
    assert set(schema["required"]) - {"schema_version"} == {f.name for f in fields(Notification)}


def test_merge_sql_covers_every_column_and_unpacks_nested_records() -> None:
    contract = next(t for t in load_table_contracts() if t["name"] == "alert_state")
    sql = merge_sql("p.alacati_ops.alert_state", contract_schema(contract), "event_id", True)
    for name in columns("alert_state"):
        assert f"AS {name}" in sql
    assert "SELECT AS STRUCT JSON_VALUE(x0, '$.region_id') AS region_id" in sql
    assert "ON t.event_id = s.event_id" in sql
    assert "WHEN MATCHED THEN UPDATE SET hazard = s.hazard" in sql  # key is never updated
    assert ", event_id = s.event_id" not in sql
    insert_only = merge_sql("p.d.t", contract_schema(contract), "event_id", False)
    assert "WHEN MATCHED" not in insert_only


def test_json_row_serialises_timestamps_and_nested_lists() -> None:
    state = first_event(
        datetime(2026, 11, 5, 12, tzinfo=UTC), datetime(2026, 11, 5, 15, tzinfo=UTC)
    )
    row = _to_json_row(state)
    assert row["start_utc"] == "2026-11-05 12:00:00.000000+00:00"
    assert row["ended_at"] is None
    assert row["region_levels"] == [{"region_id": "alacati", "level": "orange"}]
    assert row["affected_regions"] == ["alacati"]
    assert set(row) == set(columns("alert_state"))
