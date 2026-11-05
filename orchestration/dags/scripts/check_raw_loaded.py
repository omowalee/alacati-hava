"""Is ingestion run <run_id> fully in the BigQuery raw tables? (not a DAG; .airflowignore)

Run with the ingestion venv's Python (it has google-cloud-bigquery and the settings):
    /opt/venvs/ingestion/bin/python check_raw_loaded.py <run_id>

Loaded = count(distinct message_id) of the run in raw forecast_hourly + marine_hourly
equals ops.ingest_runs.succeeded_messages (messages the producer published).
Exit codes: 0 loaded, 10 not yet (or the ingest_runs row is missing), 1 error.
Last stdout line: RAW_LOADED=<loaded>/<expected>.
"""

from __future__ import annotations

import sys

from alacati_ingestion.settings import bigquery_settings
from google.cloud import bigquery

NOT_YET = 10
# raw tables are partitioned by fetched_at, ingest_runs by started_at: scan two days only.
LOOKBACK = "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: check_raw_loaded.py <run_id>", file=sys.stderr)
        return 1
    run_id = argv[1]
    s = bigquery_settings()
    raw, ops = s.datasets["raw"], s.datasets["ops"]
    sql = f"""
        SELECT
          (SELECT succeeded_messages FROM `{s.project_id}.{ops}.ingest_runs`
           WHERE run_id = @run_id AND started_at >= {LOOKBACK}
           ORDER BY finished_at DESC LIMIT 1) AS expected,
          (SELECT COUNT(DISTINCT message_id) FROM (
             SELECT message_id FROM `{s.project_id}.{raw}.forecast_hourly`
             WHERE run_id = @run_id AND fetched_at >= {LOOKBACK}
             UNION ALL
             SELECT message_id FROM `{s.project_id}.{raw}.marine_hourly`
             WHERE run_id = @run_id AND fetched_at >= {LOOKBACK})) AS loaded
    """  # noqa: S608 - identifiers come from settings, the value is a query parameter
    client = bigquery.Client(project=s.project_id, location=s.location)
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
    )
    row = next(iter(client.query(sql, job_config=config).result()))
    expected, loaded = row["expected"], int(row["loaded"] or 0)
    print(f"RAW_LOADED={loaded}/{expected if expected is not None else '?'}")
    if expected is None:
        return NOT_YET
    return 0 if loaded >= int(expected) else NOT_YET


if __name__ == "__main__":
    sys.exit(main(sys.argv))
