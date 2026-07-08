"""
Lakeflow Spark Declarative Pipeline — silver layer

Produces silver.tfl.customer_journeys as a streaming table (SCD Type 1):
the latest disruption state per customer-line pair, maintained by
dlt.apply_changes() on keys (customer_id, line_id) sequenced by _updated_at.

A streaming table (Delta-backed) is required — not a materialized view —
so that Unity Catalog column masks can be applied to PII columns by the
governance-setup job. Materialized views are view objects in UC and do not
support ALTER TABLE ... ALTER COLUMN ... SET MASK.

_inserted_at/_updated_at are propagated from the bronze source rows (see
customer_journeys_raw), not stamped with current_timestamp() at merge time —
this makes freshness reflect the age of the underlying TfL/profile data
through every layer, so a bronze ingestion failure shows up as staleness
here too instead of being masked by a fresh transform run. apply_changes()
no longer needs except_column_list — the propagated value is naturally
idempotent across merges of unchanged source data, and correctly advances
only when the underlying bronze fact actually changes.
"""

import dlt
from pyspark.sql import functions as F


_EMAIL_RE = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"


# customer_journeys is personalised-alerting-linkage purpose (identity +
# home_station/location + behavioural link to specific disruptions) — the
# most sensitive of the three retention purposes, so the shortest window:
# 1 year, once the alert this data exists to power has been generated.
# Deliberately differs from bronze's 2-year operational/profile constants —
# see CLAUDE.md's purpose-based retention table.
#
# _inserted_at/_updated_at are NOT set here — they're propagated from the
# bronze source rows in customer_journeys_raw (see below) so freshness
# reflects the age of the underlying data, not when this transform happened
# to run. Only _delete_at (retention) is a platform-computed value.
def _with_retention(df, retention_days=365):
    return df.withColumn("_delete_at", F.date_add(F.current_date(), retention_days).cast("timestamp"))


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
            profiles.telephone_number,
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
            # Take the oldest/least-fresh of the two contributing bronze rows,
            # not the newest — a fresh profiles row must never mask a stale
            # arrivals row (or vice versa) if one side's ingestion has failed.
            F.least(profiles._inserted_at, disrupted._inserted_at).alias("_inserted_at"),
            F.least(profiles._updated_at, disrupted._updated_at).alias("_updated_at"),
        )
        # Data quality filters: equivalent to expect_or_drop — invalid rows are
        # dropped here so apply_changes never processes them.
        .filter(F.col("home_station").isNotNull())
        .filter(F.col("email").rlike(_EMAIL_RE))
        .filter(
            (F.col("date_of_birth") < F.current_date())
            & (F.col("date_of_birth") > F.lit("1900-01-01").cast("date"))
        )
        .withColumn(
            "age",
            F.floor(F.months_between(F.current_date(), F.col("date_of_birth")) / 12).cast("int"),
        )
        .transform(_with_retention)
    )


dlt.create_streaming_table(
    name="customer_journeys",
    comment=(
        "Latest disruption state per customer-line pair. "
        "Contains PII (full_name, email, date_of_birth, telephone_number, home_postcode) — see ABAC policy. "
        "age is derived from date_of_birth at ingest time. "
        "Liquid-clustered on home_station and customer_id."
    ),
    cluster_by=["home_station", "customer_id"],
    # platform.freshness_sla is set here rather than via governance SQL's
    # ALTER TABLE — Databricks rejects ALTER (STREAMING) TABLE SET
    # TBLPROPERTIES against pipeline-managed streaming tables entirely;
    # table properties on these can only be set from the pipeline definition.
    table_properties={"delta.enableChangeDataFeed": "true", "platform.freshness_sla": "1d"},
)

dlt.apply_changes(
    target="customer_journeys",
    source="customer_journeys_raw",
    keys=["customer_id", "line_id"],
    sequence_by=F.col("_updated_at"),
    stored_as_scd_type=1,
)
