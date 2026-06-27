from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceDoesNotExist

w = WorkspaceClient()

MONITORS = [
    {
        "table_name": "silver.default.customer_journeys",
        "assets_dir": "/Shared/monitors/customer_journeys",
        "output_schema_name": "silver.default",
        "time_series": {"timestamp_col": "ingested_at", "granularities": ["1 day"]},
    },
    {
        "table_name": "gold.default.disruption_summary",
        "assets_dir": "/Shared/monitors/disruption_summary",
        "output_schema_name": "gold.default",
        "time_series": {"timestamp_col": "disruption_date", "granularities": ["1 day"]},
    },
]

SCHEDULE = {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "Europe/London"}

for m in MONITORS:
    table = m["table_name"]
    try:
        w.api_client.do("GET", f"/api/2.1/unity-catalog/tables/{table}/monitor")
        print(f"Monitor already exists: {table}")
    except (NotFound, ResourceDoesNotExist):
        print(f"Creating monitor: {table}")
        w.api_client.do(
            "POST",
            f"/api/2.1/unity-catalog/tables/{table}/monitor",
            body={
                "assets_dir": m["assets_dir"],
                "output_schema_name": m["output_schema_name"],
                "time_series": m["time_series"],
                "schedule": SCHEDULE,
            },
        )
        print(f"Created: {table}")
