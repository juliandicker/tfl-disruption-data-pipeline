"""
Negative access test — bronze catalog must be unreachable by reader groups.

Runs locally against the authenticated Databricks workspace. Requires:
  - Databricks CLI authenticated (databricks auth login)
  - databricks-sdk installed (pip install databricks-sdk)

Usage:
    python tests/test_bronze_access.py

Asserts that neither sg-dbplat-standard-readers nor sg-dbplat-pii-readers
appears in the grant list for the bronze catalog. This validates the zero-grant
policy on bronze described in the pipeline architecture.
"""

import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

READER_GROUPS = {"sg-dbplat-standard-readers", "sg-dbplat-pii-readers"}


def get_warehouse_id(w: WorkspaceClient) -> str:
    """Returns the first available running or starter SQL warehouse."""
    warehouses = list(w.warehouses.list())
    for wh in warehouses:
        if wh.state and wh.state.value in ("RUNNING", "STARTING", "STOPPED"):
            return wh.id
    raise RuntimeError(
        "No SQL warehouse found. Create a SQL warehouse in the workspace before running this test."
    )


def run_query(w: WorkspaceClient, warehouse_id: str, statement: str) -> list[dict]:
    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="30s",
    )
    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(
            f"Query failed: {response.status.error.message if response.status.error else 'unknown error'}"
        )
    columns = [c.name for c in response.manifest.schema.columns]
    rows = response.result.data_array or []
    return [dict(zip(columns, row)) for row in rows]


def main():
    w = WorkspaceClient()
    warehouse_id = get_warehouse_id(w)
    print(f"Using SQL warehouse: {warehouse_id}")

    rows = run_query(w, warehouse_id, "SHOW GRANTS ON CATALOG bronze")
    granted_principals = {row.get("Principal", "") for row in rows}

    violations = granted_principals & READER_GROUPS
    if violations:
        print(f"FAIL — Reader groups found in bronze grants: {violations}", file=sys.stderr)
        sys.exit(1)

    print(f"PASS — No reader group grants on bronze catalog. Principals with access: {granted_principals or 'none'}")


if __name__ == "__main__":
    main()
