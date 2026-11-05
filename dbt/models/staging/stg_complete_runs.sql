-- Ingestion runs that finished completely. Partial / failed / running runs are ignored
-- by every mart (contracts/bigquery/tables.yaml: complete_run_rule).
-- One extra day of started_at margin: a row fetched inside the lookback window may belong
-- to a run that started slightly before it.
select
    run_id,
    started_at,
    finished_at,
    past_hours,
    expected_messages,
    succeeded_messages
from {{ source('ops', 'ingest_runs') }}
where status = 'complete'
  and started_at >= timestamp_sub({{ now_utc() }}, interval {{ var('staging_lookback_days') + 1 }} day)
