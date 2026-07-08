"""
Lakeflow Spark Declarative Pipeline — gold layer

Produces:
  - gold.travel.disruption_summary: aggregated counts, no PII. Materialized view
    is correct here — aggregations re-compute naturally on refresh and no UC
    column masks are needed.
  - gold.travel.notification_targets: one row per affected customer per disrupted
    line, ready for alerting. Contains PII (full_name, email). Must be a
    streaming table (Delta-backed) so UC column masks can be applied by the
    governance-setup job.

_inserted_at/_updated_at are propagated from silver.tfl.customer_journeys
(itself propagated from bronze — see silver.py), not stamped with
current_timestamp(). disruption_summary aggregates many customer_journeys
rows per group, so it takes MIN() across the group rather than MAX() — a
single stale contributing row must surface as staleness, not get masked by
fresher rows in the same bucket. The existing `last_updated` business column
(MAX, "most recent change in this group") is a separate, legitimate concept
and is kept alongside the platform `_updated_at`.
"""

import dlt
from pyspark.sql import functions as F

KNOWN_LINE_IDS = [
    "bakerloo", "central", "circle", "district", "elizabeth",
    "hammersmith-city", "jubilee", "metropolitan", "northern",
    "piccadilly", "victoria", "waterloo-city",
]


# retention_days is required, not defaulted: disruption_summary and
# notification_targets serve different retention purposes (operational/no-PII
# vs. personalised-alerting-linkage) and previously shared one default by
# accident. See CLAUDE.md's purpose-based retention table.
def _with_retention(df, retention_days):
    return df.withColumn("_delete_at", F.date_add(F.current_date(), retention_days).cast("timestamp"))


@dlt.table(
    name="disruption_summary",
    comment=(
        "Aggregated disruption counts by line and day. No PII. "
        "Data quality expectations enforce timestamp validity and known line names."
    ),
    # platform.freshness_sla is set here rather than via governance SQL's
    # ALTER TABLE — Databricks rejects TBLPROPERTIES changes made via ALTER
    # against pipeline-managed objects (materialized views included); table
    # properties on these can only be set from the pipeline definition.
    table_properties={"platform.freshness_sla": "1h"},
)
@dlt.expect("disruption_date_not_null", "disruption_date IS NOT NULL")
@dlt.expect(
    "valid_line_id",
    f"line_id IN ({', '.join(repr(l) for l in KNOWN_LINE_IDS)})",
)
def disruption_summary():
    silver = spark.conf.get("silver_catalog", "silver")
    schema = spark.conf.get("schema", "tfl")
    return (
        spark.table(f"{silver}.{schema}.customer_journeys")
        .withColumn("disruption_date", F.to_date("_updated_at"))
        .groupBy("line_id", "line_name", "disruption_date", "status_severity_description")
        .agg(
            F.count("customer_id").alias("affected_customers"),
            F.countDistinct("home_station").alias("affected_stations"),
            F.first("disruption_description").alias("disruption_description"),
            F.max("_updated_at").alias("last_updated"),
            # Worst-case (oldest) contributing row in this group — a single
            # stale input must surface as staleness, not be masked by fresher
            # rows aggregated into the same bucket.
            F.min("_inserted_at").alias("_inserted_at"),
            F.min("_updated_at").alias("_updated_at"),
        )
        # Operational/no-PII purpose — matches tfl_arrivals' 2-year window.
        .transform(lambda df: _with_retention(df, retention_days=365 * 2))
    )


@dlt.view(name="notification_targets_raw")
def notification_targets_raw():
    silver = spark.conf.get("silver_catalog", "silver")
    schema = spark.conf.get("schema", "tfl")
    return (
        dlt.read_stream(f"{silver}.{schema}.customer_journeys")
        .select(
            "customer_id",
            "full_name",
            "email",
            "home_station",
            "line_id",
            "line_name",
            "status_severity_description",
            "disruption_reason",
            "disruption_description",
            # No fan-in here — straight passthrough of customer_journeys'
            # already-propagated (bronze-derived) timestamps.
            "_inserted_at",
            "_updated_at",
        )
        # Personalised-alerting-linkage purpose — shortest window (1 year):
        # purpose is fulfilled once the alert is generated.
        .transform(lambda df: _with_retention(df, retention_days=365))
    )


dlt.create_streaming_table(
    name="notification_targets",
    comment=(
        "One row per affected customer per disrupted line, ready for alerting. "
        "Contains email — aggregation does not equal anonymisation. "
        "ABAC masking policy applies to full_name and email for standard-readers."
    ),
    # platform.freshness_sla is set here rather than via governance SQL's
    # ALTER TABLE — see disruption_summary above for why.
    table_properties={"delta.enableChangeDataFeed": "true", "platform.freshness_sla": "1d"},
)

dlt.apply_changes(
    target="notification_targets",
    source="notification_targets_raw",
    keys=["customer_id", "line_id"],
    sequence_by=F.col("_updated_at"),
    stored_as_scd_type=1,
)
