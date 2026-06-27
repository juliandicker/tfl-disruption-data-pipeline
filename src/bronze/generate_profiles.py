"""
Synthetic customer profile generator → bronze.tfl.customer_profiles

Generates fake-but-PII-shaped traveller profiles using the Faker library.
These represent hypothetical TfL contactless-card registrations and are used
to demonstrate personalised disruption alerting and ABAC governance.

THIS IS SYNTHETIC DATA. No real customer information is used or implied.

The table is overwritten on each run (not appended) because profiles are
single-generation synthetic data — there is no genuine change stream to track.
CDC is excluded by design; see README for rationale.
"""

import argparse
import json
import uuid
from datetime import datetime, timezone

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

_parser = argparse.ArgumentParser()
_parser.add_argument("--catalog", default="bronze")
_parser.add_argument("--schema", default="tfl")
_args, _ = _parser.parse_known_args()
fake = Faker("en_GB")
Faker.seed(0)  # reproducible within a single run; seed resets on each job execution

# Must stay in sync with STATION_LINES in ingest_tfl.py so the silver join finds matches.
TFL_STATIONS = [
    "Baker Street", "Bank", "Barbican", "Bermondsey", "Bethnal Green",
    "Bond Street", "Borough", "Brixton", "Canary Wharf", "Cannon Street",
    "Clapham Common", "Clapham North", "Clapham South", "Covent Garden",
    "Earl's Court", "Elephant & Castle", "Embankment", "Euston",
    "Farringdon", "Finsbury Park", "Green Park", "Hammersmith",
    "Highbury & Islington", "Highgate", "Holborn", "Hyde Park Corner",
    "Kennington", "Kentish Town", "Kilburn", "King's Cross St. Pancras",
    "Knightsbridge", "Leicester Square", "Liverpool Street", "London Bridge",
    "Marble Arch", "Mile End", "Moorgate", "Old Street", "Oxford Circus",
    "Paddington", "Pimlico", "Putney Bridge", "Seven Sisters",
    "Shepherd's Bush", "Sloane Square", "Southwark", "Stockwell",
    "Stratford", "Temple", "Tottenham Court Road", "Tower Hill",
    "Vauxhall", "Victoria", "Waterloo", "Westminster",
]

PROFILE_COUNT = 500


def _generate() -> dict:
    dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
    return {
        "customer_id":   str(uuid.uuid4()),
        "full_name":     fake.name(),
        "email":         fake.email(),
        "date_of_birth": dob.isoformat(),
        "home_postcode": fake.postcode(),
        "card_id":       fake.numerify("############"),
        "home_station":  fake.random_element(TFL_STATIONS),
    }


def main():
    catalog = _args.catalog
    schema = _args.schema
    table = f"{catalog}.{schema}.customer_profiles"
    ingested_at = datetime.now(timezone.utc)
    profiles = [_generate() for _ in range(PROFILE_COUNT)]
    rows = [{**p, "raw_payload": json.dumps(p), "ingested_at": ingested_at} for p in profiles]

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            raw_payload     STRING  COMMENT 'Verbatim synthetic profile record as JSON.',
            customer_id     STRING,
            full_name       STRING,
            email           STRING,
            date_of_birth   DATE,
            home_postcode   STRING,
            card_id         STRING,
            home_station    STRING,
            ingested_at     TIMESTAMP
        )
        COMMENT 'SYNTHETIC DATA — generated via Faker (en_GB). Represents hypothetical TfL contactless-card registrations. Not real customer data.'
    """)

    df = (
        spark.createDataFrame(rows)
        .withColumn("ingested_at", F.col("ingested_at").cast("timestamp"))
        .withColumn("date_of_birth", F.col("date_of_birth").cast("date"))
    )

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"Wrote {PROFILE_COUNT} synthetic profiles to {table}")


main()
