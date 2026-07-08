"""
One-off local tool: export bronze.tfl.tfl_arrivals to a local Parquet file via
the Databricks SQL Statement Execution API, so it can be captured as seed data
before a workspace teardown.

Runs entirely on your machine using your own Databricks CLI/SDK credentials —
no script upload, no cluster, no job submission. This works despite bronze
having zero group grants because Unity Catalog access control is enforced per
identity regardless of compute type, and an admin identity carries implicit
full privileges.

Only tfl_arrivals is captured — see reload_bronze_seed.py's docstring for why
customer_profiles isn't seeded.

Usage:
    pip install databricks-sdk pandas pyarrow
    python tools/seed_data/export_bronze_seed.py --profile <cli-profile>
"""

import argparse
from pathlib import Path

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

_parser = argparse.ArgumentParser()
_parser.add_argument("--profile", default=None, help="Databricks CLI profile to authenticate with.")
_parser.add_argument("--warehouse-name", default="travel-sql-warehouse")
_parser.add_argument("--catalog", default="bronze")
_parser.add_argument("--schema", default="tfl")
_parser.add_argument("--output", default=str(REPO_ROOT / "seed_data" / "bronze" / "tfl_arrivals.parquet"))
args = _parser.parse_args()


def _resolve_warehouse_id(w: WorkspaceClient) -> str:
    for wh in w.warehouses.list():
        if wh.name == args.warehouse_name:
            return wh.id
    raise SystemExit(f"No SQL warehouse named '{args.warehouse_name}' found.")


def _run_query(w: WorkspaceClient, warehouse_id: str, statement: str):
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        catalog=args.catalog,
        schema=args.schema,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout="50s",
    )

    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        resp = w.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        raise SystemExit(f"Query failed ({resp.status.state}): {resp.status.error}")

    columns = [c.name for c in resp.manifest.schema.columns]
    types = [c.type_name.value for c in resp.manifest.schema.columns]
    rows = list(resp.result.data_array or [])

    next_index = resp.result.next_chunk_index
    while next_index is not None:
        chunk = w.statement_execution.get_statement_result_chunk_n(resp.statement_id, next_index)
        rows.extend(chunk.data_array or [])
        next_index = chunk.next_chunk_index

    return columns, types, rows


def main():
    w = WorkspaceClient(profile=args.profile)
    warehouse_id = _resolve_warehouse_id(w)

    columns, types, rows = _run_query(
        w, warehouse_id, f"SELECT * FROM {args.catalog}.{args.schema}.tfl_arrivals"
    )

    df = pd.DataFrame(rows, columns=columns)
    for col, type_name in zip(columns, types):
        if type_name == "TIMESTAMP":
            df[col] = pd.to_datetime(df[col], utc=True)
        elif type_name == "DATE":
            df[col] = pd.to_datetime(df[col]).dt.date
        elif type_name == "INT":
            df[col] = df[col].astype("Int64")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    print(f"Exported {len(df)} rows from {args.catalog}.{args.schema}.tfl_arrivals to {output_path}")


main()
