# Data contracts

**Read-only.** Any change needs Numan's explicit approval (see CLAUDE.md, rule 1).
Breaking changes create a new version (`*.v2.schema.json`, `schema_version: 2`);
both versions are supported until every producer and consumer has moved.

| File | What it describes |
|---|---|
| `kafka/weather_raw.v1.schema.json` | One Open-Meteo response (forecast location or marine zone) per message |
| `kafka/weather_alert.v1.schema.json` | One alert lifecycle change: `new` / `updated` / `ended` |
| `bigquery/tables.yaml` | Datasets, grain, unique keys, partitioning, clustering |
| `bigquery/raw/*.json` | Raw hourly rows, flattened from `weather.raw` |
| `bigquery/ops/*.json` | API call log, alert state, notification audit log |
| `bigquery/marts/*.json` | dbt outputs read by Streamlit and the Telegram bot |

## Conventions
- All timestamps are **UTC** (`TIMESTAMP`, ISO 8601 with `Z` in JSON). Open-Meteo
  returns `timezone=GMT` times without an offset (`2026-11-05T00:00`); the client
  appends `Z`. Local time (Europe/Istanbul) only appears in `*_local DATETIME` mart
  columns, for display.
- **Hour semantics:** `time_utc` is the Open-Meteo timestamp as returned. Wind, gusts,
  CAPE, waves and weather_code are instantaneous at that time; `precipitation` is the
  sum of the *preceding* hour. Alert windows convert this to intervals (see
  `config/alerts.yaml`).
- Only the latest run with `ops.ingest_runs.status = 'complete'` feeds marts and alerts.
- Idempotency: raw loads use deterministic job ids; alert notification ids are
  `uuid5(event_id, revision)`; ops tables are written with `MERGE` on their unique key;
  the bot checks `ops.alert_deliveries` / `ops.summary_log` before sending.
- Units are Open-Meteo's as requested in `config/ingestion.yaml`: km/h, mm, m, s, °, J/kg.
- Wind and wave directions are the direction they come **from** (meteorological).
- IDs are lowercase `snake_case` and come from `config/locations.yaml`.

## Kafka topics (Phase 3)
| Topic | Key | Partitions | Retention | Producers → consumers |
|---|---|---|---|---|
| `weather.raw` | `<source>:<point_id>` | 3 | 7 days | ingestion producer → `bq-loader`, `alert-engine` |
| `weather.alerts` | `event_id` | 1 | 30 days | alert engine → `telegram-bot` |

- Single broker (KRaft), replication factor 1 — fine for a portfolio, not for production.
- **At-least-once delivery.** Consumers commit offsets only after their side effect
  succeeded (BigQuery load finished / Telegram accepted the message). Duplicates are
  removed downstream: raw rows by `message_id`, notifications by `notification_id`.
  This is what makes "stop a consumer, start it again, no data loss" true.
- `run_expected_messages` lets a consumer know when an ingestion run is complete
  (e.g. the BigQuery consumer loads one batch per run).
