from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import MonitorCronSchedule, MonitorTimeSeries

w = WorkspaceClient()

MONITORS = [
    {
        "table_name": "silver.default.customer_journeys",
        "assets_dir": "/Shared/monitors/customer_journeys",
        "output_schema_name": "silver.default",
        "time_series": MonitorTimeSeries(timestamp_col="ingested_at", granularities=["1 day"]),
    },
    {
        "table_name": "gold.default.disruption_summary",
        "assets_dir": "/Shared/monitors/disruption_summary",
        "output_schema_name": "gold.default",
        "time_series": MonitorTimeSeries(timestamp_col="disruption_date", granularities=["1 day"]),
    },
]

schedule = MonitorCronSchedule(
    quartz_cron_expression="0 0 6 * * ?",
    timezone_id="Europe/London",
)

for m in MONITORS:
    table = m["table_name"]
    try:
        w.quality_monitors.get(table_name=table)
        print(f"Monitor already exists: {table}")
    except NotFound:
        print(f"Creating monitor: {table}")
        w.quality_monitors.create(
            table_name=table,
            assets_dir=m["assets_dir"],
            output_schema_name=m["output_schema_name"],
            time_series=m["time_series"],
            schedule=schedule,
        )
        print(f"Created: {table}")
