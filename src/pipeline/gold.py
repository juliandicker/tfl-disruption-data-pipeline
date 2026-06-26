"""
Lakeflow Spark Declarative Pipeline — gold layer

Reads silver.default.customer_journeys and produces:
  - gold.default.disruption_summary: aggregated counts, no PII.
  - gold.default.notification_targets: one row per affected customer ready for alerting.
    Still contains email because the alerting use case requires it.
    Aggregation does not equal anonymisation — this table remains PII-bearing.

This pipeline targets the gold catalog (set in gold_pipeline.yml).
"""

import dlt
from pyspark.sql import functions as F

KNOWN_LINE_IDS = [
    "bakerloo", "central", "circle", "district", "elizabeth",
    "hammersmith-city", "jubilee", "metropolitan", "northern",
    "piccadilly", "victoria", "waterloo-city",
]


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
    return (
        spark.table("silver.default.customer_journeys")
        .filter(F.col("line_id").isNotNull())
        .withColumn("disruption_date", F.to_date("ingested_at"))
        .groupBy("line_id", "line_name", "disruption_date", "status_severity_description")
        .agg(
            F.count("customer_id").alias("affected_customers"),
            F.countDistinct("home_station").alias("affected_stations"),
            F.first("disruption_description").alias("disruption_description"),
            F.max("ingested_at").alias("last_updated"),
        )
    )


@dlt.table(
    name="notification_targets",
    comment=(
        "One row per affected customer per disrupted line, ready for alerting. "
        "Contains email — aggregation does not equal anonymisation. "
        "ABAC masking policy applies to email for standard-readers."
    ),
)
def notification_targets():
    return (
        spark.table("silver.default.customer_journeys")
        .filter(F.col("line_id").isNotNull())
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
            "ingested_at",
        )
        .distinct()
    )
