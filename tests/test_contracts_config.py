"""Consistency checks between contracts/ and config/.

Run: uv run --no-project --python 3.12 --with pytest,pyyaml,jsonschema,rfc3339-validator python -m pytest tests -q
"""
import json
import uuid
from pathlib import Path

import jsonschema
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"
CONFIG = ROOT / "config"


def load_yaml(name: str) -> dict:
    return yaml.safe_load((CONFIG / name).read_text(encoding="utf-8"))


def load_json(rel: str):
    return json.loads((CONTRACTS / rel).read_text(encoding="utf-8"))


RAW = load_json("kafka/weather_raw.v1.schema.json")
ALERT = load_json("kafka/weather_alert.v1.schema.json")
LOCATIONS = load_yaml("locations.yaml")
INGESTION = load_yaml("ingestion.yaml")["open_meteo"]
ALERTS = load_yaml("alerts.yaml")
SURF = load_yaml("surf.yaml")
TABLES = yaml.safe_load((CONTRACTS / "bigquery/tables.yaml").read_text(encoding="utf-8"))

FORMAT_CHECKER = jsonschema.Draft202012Validator.FORMAT_CHECKER


def validate(schema: dict, instance: dict) -> None:
    jsonschema.Draft202012Validator(schema, format_checker=FORMAT_CHECKER).validate(instance)


@pytest.mark.parametrize("schema", [RAW, ALERT])
def test_kafka_schemas_are_valid_json_schema(schema):
    jsonschema.Draft202012Validator.check_schema(schema)


def _raw_message(source: str, hour: dict) -> dict:
    return {
        "schema_version": 1,
        "message_id": str(uuid.uuid4()),
        "run_id": str(uuid.uuid4()),
        "run_expected_messages": 17,
        "source": source,
        "point_id": "ilica" if source == "forecast" else "north",
        "fetched_at": "2026-11-05T12:00:05Z",
        "request": {"latitude": 38.311, "longitude": 26.37, "forecast_days": 3, "past_hours": 3},
        "grid": {"latitude": 38.29, "longitude": 26.41, "elevation_m": 0.0},
        "model": "best_match",
        "units": {"wind_speed_10m": "km/h"},
        "hourly": [{"time_utc": "2026-11-05T12:00:00Z", **hour}],
    }


def test_raw_forecast_message_validates():
    validate(RAW, _raw_message("forecast", {"wind_speed_10m": 31.2, "weather_code": 95, "cape": None}))


def test_raw_marine_message_validates():
    validate(RAW, _raw_message("marine", {"wave_height": 1.02, "wave_period": None}))


def test_raw_rejects_marine_variable_in_forecast_message():
    with pytest.raises(jsonschema.ValidationError):
        validate(RAW, _raw_message("forecast", {"wave_height": 1.0}))


def _alert(status: str, previous_level, revision: int = 1, reasons=None, **overrides):
    is_new = status == "new"
    msg = {
        "schema_version": 1,
        "notification_id": str(uuid.uuid4()),
        "event_id": str(uuid.uuid4()),
        "revision": revision,
        "status": status,
        "change_reasons": reasons or (["first_detected"] if is_new else ["level_up"]),
        "hazard": "thunderstorm",
        "level": "orange",
        "previous_level": previous_level,
        "start_utc": "2026-11-05T12:00:00Z",
        "end_utc": "2026-11-05T17:00:00Z",
        "previous_start_utc": None if is_new else "2026-11-05T13:00:00Z",
        "previous_end_utc": None if is_new else "2026-11-05T16:00:00Z",
        "peak_value": 20.0,
        "peak_unit": "mm/h",
        "peak_time_utc": "2026-11-05T14:00:00Z",
        "affected_regions": ["alacati", "ilica"],
        "region_levels": [{"region_id": "alacati", "level": "orange"}, {"region_id": "ilica", "level": "yellow"}],
        "affected_locations": ["alacati_surf", "ilica"],
        "forecast_run_id": str(uuid.uuid4()),
        "issued_at": "2026-11-05T09:00:00Z",
    }
    msg.update(overrides)
    return msg


def test_alert_new_updated_ended_validate():
    validate(ALERT, _alert("new", None))
    validate(ALERT, _alert("updated", "yellow", revision=2))
    validate(ALERT, _alert("ended", "orange", revision=3, reasons=["forecast_cleared"]))


@pytest.mark.parametrize(
    "bad",
    [
        _alert("new", "yellow"),                                   # new with previous level
        _alert("new", None, revision=2),                           # new must be revision 1
        _alert("updated", None, revision=2),                       # updated needs previous level
        _alert("updated", "yellow", revision=1),                   # updated needs revision >= 2
        _alert("updated", "yellow", revision=2, previous_start_utc=None),
        _alert("new", None, issued_at="yesterday"),                # proves date-time format is checked
        _alert("ended", "orange", revision=3, reasons=["level_up"]),      # ended needs an end reason
        _alert("updated", "yellow", revision=2, reasons=["window_passed"]),  # updated needs a change reason
    ],
)
def test_alert_invalid_messages_rejected(bad):
    with pytest.raises(jsonschema.ValidationError):
        validate(ALERT, bad)


def test_notification_id_is_deterministic():
    # Contract: uuid5(NAMESPACE_URL, 'alacati-hava:' + event_id + ':' + revision)
    event_id = "6f1c2b9e-1d2a-4c7e-9b1f-0a2b3c4d5e6f"
    a = uuid.uuid5(uuid.NAMESPACE_URL, f"alacati-hava:{event_id}:2")
    b = uuid.uuid5(uuid.NAMESPACE_URL, f"alacati-hava:{event_id}:2")
    assert a == b
    validate(ALERT, _alert("updated", "yellow", revision=2, notification_id=str(a), event_id=event_id))


def test_ingestion_variables_match_raw_contract():
    fc_props = set(RAW["$defs"]["forecast_hour"]["properties"]) - {"time_utc"}
    mr_props = set(RAW["$defs"]["marine_hour"]["properties"]) - {"time_utc"}
    assert set(INGESTION["forecast_hourly"]) == fc_props
    assert set(INGESTION["marine_hourly"]) == mr_props
    # Open-Meteo counts > 10 variables as extra calls
    assert len(INGESTION["forecast_hourly"]) <= 10
    assert len(INGESTION["marine_hourly"]) <= 10


def test_ingestion_variables_match_bigquery_raw_columns():
    fc_cols = {c["name"] for c in load_json("bigquery/raw/forecast_hourly.json")}
    mr_cols = {c["name"] for c in load_json("bigquery/raw/marine_hourly.json")}
    assert set(INGESTION["forecast_hourly"]) <= fc_cols
    assert set(INGESTION["marine_hourly"]) <= mr_cols


def test_every_table_schema_file_exists_and_partition_fields_exist():
    for t in TABLES["tables"]:
        cols = {c["name"]: c for c in load_json(f"bigquery/{t['schema']}")}
        for key in t["unique_key"] + t["cluster_by"]:
            assert key in cols, f"{t['name']}: {key} missing"
        if t["partition"]:
            assert cols[t["partition"]["field"]]["type"] in {"TIMESTAMP", "DATE", "DATETIME"}


def test_location_references_are_consistent():
    regions = {r["id"] for r in LOCATIONS["regions"]}
    zones = {z["id"] for z in LOCATIONS["marine_zones"]}
    ids = [loc["id"] for loc in LOCATIONS["locations"]]
    assert len(ids) == len(set(ids)), "duplicate location id"
    for loc in LOCATIONS["locations"]:
        assert loc["region"] in regions, loc["id"]
        assert loc["marine_zone"] in zones, loc["id"]
        assert loc["type"] in {"beach", "surf_spot"}
        assert 0 <= loc["shore_facing_deg"] < 360
        # Rule 4: nothing is verified until Numan says so — the flag must always be present
        assert isinstance(loc["shore_verified"], bool)
        assert isinstance(loc["coords_verified"], bool)


def test_surf_spots_exist_in_locations():
    spots = {loc["id"] for loc in LOCATIONS["locations"] if loc["type"] == "surf_spot"}
    assert set(SURF["spots"]) <= spots


def test_alert_hazards_match_alert_contract_and_levels_increase():
    assert set(ALERTS["hazards"]) == set(ALERT["properties"]["hazard"]["enum"])
    levels = ALERTS["levels"]
    assert levels == ALERT["properties"]["level"]["enum"]
    for hazard in ALERTS["hazards"].values():
        for var, t in hazard.get("thresholds", {}).items():
            values = [t[lvl] for lvl in levels]
            assert values == sorted(values) and len(set(values)) == 3, var


# --- BigQuery schema files -------------------------------------------------------

BQ_TYPES = {"STRING", "INT64", "FLOAT64", "BOOL", "TIMESTAMP", "DATE", "DATETIME", "RECORD"}
BQ_MODES = {"NULLABLE", "REQUIRED", "REPEATED"}


def _check_fields(fields, where):
    names = [c["name"] for c in fields]
    assert len(names) == len(set(names)), f"{where}: duplicate column"
    for c in fields:
        assert c["type"] in BQ_TYPES, f"{where}.{c['name']}"
        assert c["mode"] in BQ_MODES, f"{where}.{c['name']}"
        assert c.get("description"), f"{where}.{c['name']} has no description"
        if c["type"] == "RECORD":
            _check_fields(c["fields"], f"{where}.{c['name']}")


def test_bigquery_schema_files_are_valid_and_all_listed():
    listed = {t["schema"] for t in TABLES["tables"]}
    on_disk = {p.relative_to(CONTRACTS / "bigquery").as_posix()
               for p in (CONTRACTS / "bigquery").glob("*/*.json")}
    assert listed == on_disk
    for rel in on_disk:
        _check_fields(load_json(f"bigquery/{rel}"), rel)


def _bq_cols(rel):
    return {c["name"] for c in load_json(f"bigquery/{rel}")}


def test_alert_message_fields_are_stored_in_notifications_table():
    kafka_fields = set(ALERT["properties"]) - {"schema_version"}
    assert kafka_fields <= _bq_cols("ops/alert_notifications.json")


def test_alert_state_has_lifecycle_columns():
    needed = {"revision", "notified_level", "notified_start_utc", "notified_end_utc",
              "notified_regions", "last_seen_run_id", "absent_runs", "region_levels"}
    assert needed <= _bq_cols("ops/alert_state.json")


# --- config cross-checks -----------------------------------------------------------

def test_alert_variables_exist_in_raw_or_derived():
    fc = set(INGESTION["forecast_hourly"])
    mr = set(INGESTION["marine_hourly"])
    derived = set(ALERTS.get("derived", {}))
    for d in ALERTS["derived"].values():
        assert d["source_variable"] in fc
    for name, h in ALERTS["hazards"].items():
        available = (fc if h["source"] == "forecast" else mr) | derived
        assert h["primary_variable"] in available, name
        assert h.get("peak_unit"), name
        for var in h.get("thresholds", {}):
            assert var in available, f"{name}.{var}"
        for cond in h.get("escalate", {}).values():
            assert set(cond) <= available, name
        for other in h.get("suppresses", []):
            assert other in ALERTS["hazards"], name


def test_thunderstorm_escalation_increases():
    esc = ALERTS["hazards"]["thunderstorm"]["escalate"]
    for var in esc["orange"]:
        assert esc["orange"][var] < esc["red"][var], var


def test_surf_ranges_are_sane():
    for spot, s in SURF["spots"].items():
        ride, ideal = s["wind_kmh"]["rideable"], s["wind_kmh"]["ideal"]
        assert ride["min"] < ride["max"] and ideal["min"] < ideal["max"], spot
        assert ride["min"] <= ideal["min"] and ideal["max"] <= ride["max"], spot
        for d in s["good_directions"]:
            assert 0 <= d["from_deg"] < 360 and 0 <= d["to_deg"] < 360, spot
    hours = SURF["daylight_local_hours"]
    assert 0 <= hours["start"] < hours["end"] <= 24


def test_beach_score_config_is_consistent():
    bs = load_yaml("beach_score.yaml")
    assert sum(bs["weights"].values()) == pytest.approx(1.0)
    assert min(b["min"] for b in bs["labels_tr"]) == 0
    assert bs["wind"]["calm_kmh"] < bs["wind"]["strong_kmh"]
    assert bs["wind"]["calm_gust_kmh"] < bs["wind"]["strong_gust_kmh"]
    assert bs["waves"]["calm_wave_m"] < bs["waves"]["rough_wave_m"]
    hours = bs["daytime_local_hours"]
    assert 0 <= hours["start"] < hours["end"] <= 24


def test_expected_units_cover_all_variables():
    assert set(INGESTION["expected_units"]) == set(INGESTION["forecast_hourly"]) | set(INGESTION["marine_hourly"])


def test_hourly_call_budget():
    per_run = len(LOCATIONS["locations"]) + len(LOCATIONS["marine_zones"])
    assert per_run * 24 <= INGESTION["daily_call_budget"]
    assert INGESTION["daily_call_budget"] < 10_000
