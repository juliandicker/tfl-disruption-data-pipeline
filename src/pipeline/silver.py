# Databricks notebook source
"""
Lakeflow Spark Declarative Pipeline — silver layer

Reads bronze tables and produces silver.default.customer_journeys:
a cleaned, deduplicated join of synthetic customer profiles to TfL
disruption data on home_station. Contains PII; ABAC masking is applied
by the governance-setup job on full_name, email, and home_postcode.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dlt.table(
    name="customer_journeys",
    comment=(
        "Cleaned, deduplicated join of synthetic customer profiles to TfL tube line status "
        "on home_station. Contains PII (full_name, email, home_postcode) — see ABAC policy. "
        "Liquid-clustered on home_station and customer_id."
    ),
    cluster_by=["home_station", "customer_id"],
)
@dlt.expect_or_drop(
    "valid_email",
    r"email RLIKE '^[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[A-Za-z]{2,}$'",
)
@dlt.expect_or_drop("home_station_not_null", "home_station IS NOT NULL")
@dlt.expect_or_drop(
    "dob_plausible",
    "date_of_birth < current_date() AND date_of_birth > date '1900-01-01'",
)
def customer_journeys():
    arrivals = spark.table("bronze.default.tfl_arrivals")
    profiles = spark.table("bronze.default.customer_profiles")

    # Left join so customers with no current disruption on their line are still present.
    # status_severity < 10 means something other than "Good Service" is reported.
    disrupted = arrivals.filter(F.col("status_severity") < 10)

    joined = profiles.join(
        disrupted,
        profiles.home_station == disrupted.station_name,
        "left",
    ).select(
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

    # Deduplicate: keep the most recent record per customer-line pair.
    w = Window.partitionBy("customer_id", "line_id").orderBy(F.col("ingested_at").desc())
    return (
        joined
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
