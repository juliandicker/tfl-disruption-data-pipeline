"""
Reload the committed bronze seed data (seed_data/bronze/tfl_arrivals.parquet)
back into bronze.tfl.tfl_arrivals after a workspace rebuild, via the
Databricks SQL Statement Execution API — restoring a realistic-looking
starting point before tfl-pipeline resumes live ingestion on top of it.

Runs against the SQL Statement Execution API only — no script upload, no
cluster, no job submission. Two callers:

  * A human, locally, with --profile <cli-profile>, using their own
    Databricks CLI/SDK credentials. See export_bronze_seed.py's docstring
    for why this works despite bronze's zero group grants.
  * The deploy workflow (.github/workflows/deploy.yml), with no --profile
    and --ci set, authenticating as sp-tfl-pipeline via the DATABRICKS_HOST/
    DATABRICKS_TOKEN env vars already exported by that job. That SP is the
    run_as identity for tfl_pipeline, so it already holds bronze's grants —
    no extra permissions needed. Runs after every bundle deploy but only
    ever inserts rows the first time (see --ci below).

Only tfl_arrivals is reloaded. customer_profiles is deliberately not seeded —
it's single-generation Faker output with no accumulated history worth
preserving, and generate_profiles.py fully overwrites it on every
tfl-pipeline run anyway; just let that task run after rebuild instead.

_inserted_at/_updated_at are shifted forward by a constant offset so the most
recently captured row lands at reload time "now" (freshness SLA passes
immediately), while the relative spread between rows (several manual capture
runs a few hours apart) is preserved rather than collapsing to one timestamp.
_delete_at is recomputed from the shifted _inserted_at using the same
retention window ingest_tfl.py uses, so Auto TTL stays correct.

Refuses to run against a table that already has rows unless --force is
passed, to avoid duplicating history on top of live data from an accidental
re-run. Under --ci, that refusal becomes a silent no-op (exit 0) instead of
a hard failure, since "already seeded" is the expected steady state on every
deploy after the first — it shouldn't turn the workflow red.

Rows are inserted in batches via a single multi-row parameterized INSERT
per batch (not string-interpolated SQL) — raw_payload is arbitrary JSON
text and needs safe binding, not manual quote-escaping. Batch size is
capped at 20 rows (240 named parameters) to stay under the Statement
Execution API's 255-parameter-per-statement limit — inserting one row at
a time took ~1500 round trips and 15+ minutes in CI.

Usage:
    pip install databricks-sdk pandas pyarrow
    python tools/seed_data/reload_bronze_seed.py --profile <cli-profile>
    python tools/seed_data/reload_bronze_seed.py --ci   # CI: no-op if already seeded
"""

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementParameterListItem, StatementState

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
# Operational purpose (no personal data) — mirrors ingest_tfl.py's
# RETENTION_DAYS_OPERATIONAL. See CLAUDE.md's purpose-based retention table.
RETENTION_DAYS = 365 * 2

_parser = argparse.ArgumentParser()
_parser.add_argument("--profile", default=None, help="Databricks CLI profile to authenticate with.")
_parser.add_argument("--warehouse-name", default="travel-sql-warehouse")
_parser.add_argument("--catalog", default="bronze")
_parser.add_argument("--schema", default="tfl")
_parser.add_argument("--input", default=str(REPO_ROOT / "seed_data" / "bronze" / "tfl_arrivals.parquet"))
_parser.add_argument("--force", action="store_true", help="Reload even if the target table already has rows.")
_parser.add_argument("--ci", action="store_true", help="Treat an already-seeded table as a clean no-op (exit 0) instead of failing.")
args = _parser.parse_args()

TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS {table} (
        raw_payload                 STRING    COMMENT 'Verbatim TfL API line response as JSON.',
        line_id                     STRING,
        line_name                   STRING,
        station_name                STRING,
        status_severity             INT       COMMENT '10 = Good Service; lower values indicate disruption.',
        status_severity_description STRING,
        disruption_reason           STRING,
        disruption_description      STRING,
        affected_stops_json         STRING    COMMENT 'JSON array of stop names affected by the disruption.',
        _inserted_at                TIMESTAMP COMMENT 'Platform: when this row first arrived in bronze. Immutable.',
        _updated_at                 TIMESTAMP COMMENT 'Platform: when this row was last written.',
        _delete_at                  TIMESTAMP COMMENT 'Platform: Auto TTL expiry. Raw operational data — 2-year retention.'
    )
    CLUSTER BY (line_id, _inserted_at)
    COMMENT 'TfL tube line status per station-line combination. Appended every 15 minutes.'
"""

INSERT_HEADER = """
    INSERT INTO {table} (
        raw_payload, line_id, line_name, station_name, status_severity,
        status_severity_description, disruption_reason, disruption_description,
        affected_stops_json, _inserted_at, _updated_at, _delete_at
    ) VALUES
"""

# 12 named parameters per row; the Statement Execution API caps a statement
# at 255 named parameters total, so 20 rows (240 params) leaves headroom.
BATCH_SIZE = 20


def _row_values_clause(i: int) -> str:
    return (
        f"(:raw_payload_{i}, :line_id_{i}, :line_name_{i}, :station_name_{i}, "
        f"CAST(:status_severity_{i} AS INT), :status_severity_description_{i}, "
        f":disruption_reason_{i}, :disruption_description_{i}, :affected_stops_json_{i}, "
        f"CAST(:inserted_at_{i} AS TIMESTAMP), CAST(:updated_at_{i} AS TIMESTAMP), "
        f"CAST(:delete_at_{i} AS TIMESTAMP))"
    )


def _row_parameters(i: int, row) -> list[StatementParameterListItem]:
    return [
        StatementParameterListItem(name=f"raw_payload_{i}", value=row["raw_payload"]),
        StatementParameterListItem(name=f"line_id_{i}", value=row["line_id"]),
        StatementParameterListItem(name=f"line_name_{i}", value=row["line_name"]),
        StatementParameterListItem(name=f"station_name_{i}", value=row["station_name"]),
        StatementParameterListItem(name=f"status_severity_{i}", value=str(row["status_severity"])),
        StatementParameterListItem(name=f"status_severity_description_{i}", value=row["status_severity_description"]),
        StatementParameterListItem(name=f"disruption_reason_{i}", value=row["disruption_reason"]),
        StatementParameterListItem(name=f"disruption_description_{i}", value=row["disruption_description"]),
        StatementParameterListItem(name=f"affected_stops_json_{i}", value=row["affected_stops_json"]),
        StatementParameterListItem(name=f"inserted_at_{i}", value=row["_inserted_at"].isoformat()),
        StatementParameterListItem(name=f"updated_at_{i}", value=row["_updated_at"].isoformat()),
        StatementParameterListItem(name=f"delete_at_{i}", value=row["_delete_at"].isoformat()),
    ]


def _resolve_warehouse_id(w: WorkspaceClient) -> str:
    for wh in w.warehouses.list():
        if wh.name == args.warehouse_name:
            return wh.id
    raise SystemExit(f"No SQL warehouse named '{args.warehouse_name}' found.")


def _exec(w: WorkspaceClient, warehouse_id: str, statement: str, parameters=None):
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        catalog=args.catalog,
        schema=args.schema,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout="50s",
        parameters=parameters,
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise SystemExit(f"Statement failed ({resp.status.state}): {resp.status.error}\n{statement}")
    return resp


def main():
    w = WorkspaceClient(profile=args.profile)
    warehouse_id = _resolve_warehouse_id(w)
    table = f"{args.catalog}.{args.schema}.tfl_arrivals"

    if w.tables.exists(table).table_exists:
        row_count = int(_exec(w, warehouse_id, f"SELECT COUNT(*) FROM {table}").result.data_array[0][0])
        if row_count > 0 and not args.force:
            if args.ci:
                print(f"{table} already has {row_count} rows — skipping seed (already seeded).")
                return
            raise SystemExit(f"{table} already has {row_count} rows — pass --force to reload anyway.")

    df = pd.read_parquet(args.input)
    df["_inserted_at"] = pd.to_datetime(df["_inserted_at"], utc=True)
    df["_updated_at"] = pd.to_datetime(df["_updated_at"], utc=True)

    offset = datetime.now(timezone.utc) - df["_inserted_at"].max()
    df["_inserted_at"] = df["_inserted_at"] + offset
    df["_updated_at"] = df["_updated_at"] + offset
    df["_delete_at"] = df["_inserted_at"] + timedelta(days=RETENTION_DAYS)

    _exec(w, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    _exec(w, warehouse_id, TABLE_DDL.format(table=table))

    for start in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[start:start + BATCH_SIZE]
        values_sql = ",\n        ".join(_row_values_clause(i) for i in range(len(batch)))
        parameters = [
            param
            for i, (_, row) in enumerate(batch.iterrows())
            for param in _row_parameters(i, row)
        ]
        _exec(w, warehouse_id, INSERT_HEADER.format(table=table) + values_sql, parameters=parameters)

    print(f"Reloaded {len(df)} seed rows into {table}")


main()
