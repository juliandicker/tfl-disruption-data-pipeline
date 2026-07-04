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
"""

import dlt
from pyspark.sql import functions as F

KNOWN_LINE_IDS = [
    "bakerloo", "central", "circle", "district", "elizabeth",
    "hammersmith-city", "jubilee", "metropolitan", "northern",
    "piccadilly", "victoria", "waterloo-city",
]


def _with_platform_columns(df, retention_days=365 * 7):
    return (
        df.withColumn("_inserted_at", F.current_timestamp())
        .withColumn("_updated_at", F.current_timestamp())
        .withColumn("_delete_at", F.date_add(F.current_date(), retention_days).cast("timestamp"))
    )


@dlt.table(
    name="disruption_summary",
    comment=(
        "Aggregated disruption counts by line and day. No PII. "
        "Data quality expectations enforce timestamp validity and known line names."
    ),
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
        )
        .transform(_with_platform_columns)
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
        )
        .transform(_with_platform_columns)
    )


dlt.create_streaming_table(
    name="notification_targets",
    comment=(
        "One row per affected customer per disrupted line, ready for alerting. "
        "Contains email — aggregation does not equal anonymisation. "
        "ABAC masking policy applies to full_name and email for standard-readers."
    ),
    table_properties={"delta.enableChangeDataFeed": "true"},
)

dlt.apply_changes(
    target="notification_targets",
    source="notification_targets_raw",
    keys=["customer_id", "line_id"],
    sequence_by=F.col("_updated_at"),
    stored_as_scd_type=1,
    except_column_list=["_inserted_at"],
)
