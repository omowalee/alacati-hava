"""Create the raw and ops BigQuery datasets/tables from contracts/ (idempotent).

Staging and mart datasets are created by dbt. Existing tables are left untouched;
if a table's schema differs from its contract, this reports it instead of altering it.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import yaml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from alacati_ingestion.settings import CONTRACTS_DIR, BigQuerySettings, bigquery_settings

log = logging.getLogger(__name__)

MANAGED_DATASETS = ("raw", "ops")


def _schema_field(col: dict[str, Any]) -> bigquery.SchemaField:
    return bigquery.SchemaField(
        name=col["name"],
        field_type=col["type"],
        mode=col["mode"],
        description=col.get("description") or "",
        fields=[_schema_field(c) for c in col.get("fields", [])],
    )


def load_table_contracts() -> list[dict[str, Any]]:
    with (CONTRACTS_DIR / "bigquery" / "tables.yaml").open(encoding="utf-8") as fh:
        tables: list[dict[str, Any]] = yaml.safe_load(fh)["tables"]
    return tables


def contract_schema(table: dict[str, Any]) -> list[bigquery.SchemaField]:
    path = CONTRACTS_DIR / "bigquery" / table["schema"]
    return [_schema_field(c) for c in json.loads(path.read_text(encoding="utf-8"))]


def build_table(settings: BigQuerySettings, table: dict[str, Any]) -> bigquery.Table:
    dataset = settings.datasets[table["dataset"]]
    bq_table = bigquery.Table(
        f"{settings.project_id}.{dataset}.{table['name']}", schema=contract_schema(table)
    )
    part = table.get("partition")
    if part:
        expiration = part.get("expiration_days")
        bq_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=part["field"],
            expiration_ms=expiration * 24 * 3600 * 1000 if expiration else None,
        )
    if table.get("cluster_by"):
        bq_table.clustering_fields = table["cluster_by"]
    bq_table.description = table.get("grain")
    return bq_table


# The API reports legacy type names; contracts use GoogleSQL names.
_TYPE_ALIASES = {"FLOAT": "FLOAT64", "INTEGER": "INT64", "BOOLEAN": "BOOL", "STRUCT": "RECORD"}


def _same_schema(a: list[bigquery.SchemaField], b: list[bigquery.SchemaField]) -> bool:
    def key(fields: list[bigquery.SchemaField]) -> list[tuple[Any, ...]]:
        return [
            (f.name, _TYPE_ALIASES.get(f.field_type, f.field_type), f.mode, key(list(f.fields)))
            for f in fields
        ]

    return key(a) == key(b)


def setup(settings: BigQuerySettings | None = None) -> list[str]:
    """Create missing datasets/tables. Returns a list of schema mismatches (empty = OK)."""
    settings = settings or bigquery_settings()
    client = bigquery.Client(project=settings.project_id, location=settings.location)
    mismatches: list[str] = []

    for logical in MANAGED_DATASETS:
        ds = bigquery.Dataset(f"{settings.project_id}.{settings.datasets[logical]}")
        ds.location = settings.location
        client.create_dataset(ds, exists_ok=True)
        log.info("dataset ready: %s", ds.dataset_id)

    for table in load_table_contracts():
        if table["dataset"] not in MANAGED_DATASETS:
            continue
        wanted = build_table(settings, table)
        try:
            existing = client.get_table(wanted.reference)
        except NotFound:
            client.create_table(wanted)
            log.info("created table %s", wanted.table_id)
            continue
        if not _same_schema(list(existing.schema), list(wanted.schema)):
            mismatches.append(f"{wanted.table_id}: schema differs from contract")
        else:
            log.info("table ok: %s", wanted.table_id)
    return mismatches


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    problems = setup()
    for p in problems:
        log.error(p)
    raise SystemExit(1 if problems else 0)
