"""
Lakeflow Spark Declarative Pipeline — silver layer

Produces silver.tfl.customer_journeys as a streaming table (SCD Type 1):
the latest disruption state per customer-line pair, maintained by
dlt.apply_changes() on keys (customer_id, line_id) sequenced by ingested_at.

A streaming table (Delta-backed) is required — not a materialized view —
so that Unity Catalog column masks can be applied to PII columns by the
governance-setup job. Materialized views are view objects in UC and do not
support ALTER TABLE ... ALTER COLUMN ... SET MASK.
"""

import dlt
from pyspark.sql import functions as F


_EMAIL_RE = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"


@dlt.view(name="customer_journeys_raw")
def customer_journeys_raw():
    bronze = spark.conf.get("bronze_catalog", "bronze")
    schema = spark.conf.get("schema", "tfl")
    arrivals = dlt.read_stream(f"{bronze}.{schema}.tfl_arrivals")
    profiles = spark.table(f"{bronze}.{schema}.customer_profiles")

    disrupted = arrivals.filter(F.col("status_severity") < 10)

    return (
        profiles.join(
            disrupted,
            profiles.home_station == disrupted.station_name,
            "inner",
        )
        .select(
            profiles.customer_id,
            profiles.full_name,
            profiles.email,
            profiles.date_of_birth,
            profiles.home_postcode,
            profiles.card_id,
            profiles.home_station,
            disrupted.line_id,
            disrupted.line_name,
            disrupted.status_severity,
            disrupted.status_severity_description,
            disrupted.disruption_reason,
            disrupted.disruption_description,
            disrupted.affected_stops_json,
            disrupted.ingested_at,
        )
        # Data quality filters: equivalent to expect_or_drop — invalid rows are
        # dropped here so apply_changes never processes them.
        .filter(F.col("home_station").isNotNull())
        .filter(F.col("email").rlike(_EMAIL_RE))
        .filter(
            (F.col("date_of_birth") < F.current_date())
            & (F.col("date_of_birth") > F.lit("1900-01-01").cast("date"))
        )
    )


dlt.create_streaming_table(
    name="customer_journeys",
    comment=(
        "Latest disruption state per customer-line pair. "
        "Contains PII (full_name, email, home_postcode) — see ABAC policy. "
        "Liquid-clustered on home_station and customer_id."
    ),
    cluster_by=["home_station", "customer_id"],
    table_properties={"delta.enableChangeDataFeed": "true"},
)

dlt.apply_changes(
    target="customer_journeys",
    source="customer_journeys_raw",
    keys=["customer_id", "line_id"],
    sequence_by=F.col("ingested_at"),
    stored_as_scd_type=1,
)
