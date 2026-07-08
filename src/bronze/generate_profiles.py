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
from datetime import datetime, timedelta, timezone

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

_parser = argparse.ArgumentParser()
_parser.add_argument("--catalog", default="bronze")
_parser.add_argument("--schema", default="tfl")
_args, _ = _parser.parse_known_args()
fake = Faker("en_GB")

# Purpose: customer profile/identity data powering the personalisation
# feature — bounded to that purpose rather than an unjustified blanket
# "customer data" default. customer_notes (unstructured, embeds PII) shares
# this row-level clock; splitting it onto its own, likely shorter, retention
# would need its own table and is a deliberately deferred follow-up. See
# CLAUDE.md's purpose-based retention table.
RETENTION_DAYS_CUSTOMER_PROFILE = 365 * 2
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

_STAFF_PII_TEMPLATES = [
    "Customer called to query account status. Confirmed contact email as {email}. Account updated and confirmation sent.",
    "Spoke with {name} regarding delayed Oyster card registration. Customer reachable on {phone}. Issue escalated to card operations.",
    "Disruption alert preferences updated at customer request. Email: {email}, Tel: {phone}. Changes take effect from next service window.",
    "Contact log: {name} reached via {phone}. Reported incorrect home station. Updated to {station}. Verification email sent to {email}.",
    "Account review completed. Identity verified against DOB on file. Email address confirmed as {email}.",
    "Replacement card requested by {name}. Dispatched to home postcode {postcode}. Expected delivery 3–5 working days.",
    "Fraud query raised against account. Spoke with {name} on {phone}. Confirmed recent journeys legitimate. Case closed.",
    "Customer {name} requested export of personal data under DSAR. Request logged. Response sent to {email} within statutory window.",
]

_STAFF_CLEAN_TEMPLATES = [
    "Card registered for contactless travel alerts. Station preferences saved. No further action required.",
    "Account verified following self-service registration. Welcome email dispatched.",
    "Disruption notification opted in. Alert frequency set to immediate. Service window: 06:00–23:00.",
    "Account flagged for routine quarterly compliance review. No anomalies detected. File closed.",
    "Duplicate account detected and merged. Primary record retained. Secondary record archived.",
    "Journey history reviewed at customer request. Three-month export generated and sent via secure link.",
    "Card reported lost. Replacement issued via standard process. Old card deactivated as of {date}.",
    "Travel preferences updated. Peak-hour alerts enabled. Off-peak suppressed per customer instruction.",
    "Account created via staff-assisted portal registration. Consent recorded. GDPR notice provided.",
    "Accessibility requirement noted on account. Large-print correspondence preference flagged.",
]

_CUSTOMER_PII_TEMPLATES = [
    "Hi, could you please update my contact email to {email}? My old one no longer works. Many thanks.",
    "Please note I've moved. New address is {address}, {postcode}. Can you update your records? — {name}",
    "I can be reached on {phone} between 9am and 5pm if you need to contact me about my account.",
    "My name is {name} and I commute from {station} every weekday. Please send all alerts to {email}.",
    "Could someone call me back on {phone}? I've been having trouble getting through on the main line. Thanks, {name}.",
    "I'd like to add a secondary email address {email2} for disruption alerts as I sometimes miss the ones sent to my main address.",
]

_CUSTOMER_CLEAN_TEMPLATES = [
    "Really frustrated with the delays on my line this week. This seems to happen every Monday morning without fail.",
    "The disruption alerts have been really useful lately. Thank you for improving the service.",
    "I would like to opt out of SMS notifications and receive email updates only going forward.",
    "Can you please increase the frequency of status updates during peak hours? By the time I get an alert I'm already on the train.",
    "Sometimes the notifications arrive after the disruption has already ended, which makes them less useful. Please look into this.",
    "I use this service every single day and I'm genuinely impressed by the improvements over the last year.",
    "Please can someone look into why my card is occasionally being rejected at the barriers at Waterloo? Happens about once a week.",
    "I'd appreciate a weekly summary email rather than individual alerts for every delay — it gets a bit overwhelming.",
    fake.paragraph(nb_sentences=4),
    fake.paragraph(nb_sentences=5),
]


def _generate_notes(profile: dict) -> str:
    """
    Produces multi-entry CRM-style notes for a customer account.
    Entries are dated and attributed to staff or the customer.
    Some entries contain PII drawn from the profile; others are clean.
    This mix is intentional — it exercises unstructured PII detection.
    """
    num_entries = fake.random_int(min=2, max=5)
    entries = []

    entry_types = ["staff_pii", "staff_clean", "staff_clean", "customer_pii", "customer_clean", "customer_clean"]

    for _ in range(num_entries):
        entry_type = fake.random_element(entry_types)
        date = fake.date_between(start_date="-3y", end_date="today")
        author = "Staff" if entry_type.startswith("staff") else "Customer"

        if entry_type == "staff_pii":
            text = fake.random_element(_STAFF_PII_TEMPLATES).format(
                name=profile["full_name"],
                email=profile["email"],
                phone=profile["telephone_number"],
                postcode=profile["home_postcode"],
                station=profile["home_station"],
                date=date,
            )
        elif entry_type == "staff_clean":
            text = fake.random_element(_STAFF_CLEAN_TEMPLATES).format(date=date)
        elif entry_type == "customer_pii":
            text = fake.random_element(_CUSTOMER_PII_TEMPLATES).format(
                name=profile["full_name"],
                email=profile["email"],
                email2=fake.email(),
                phone=profile["telephone_number"],
                postcode=profile["home_postcode"],
                address=fake.street_address(),
                station=profile["home_station"],
            )
        else:
            text = fake.random_element(_CUSTOMER_CLEAN_TEMPLATES)

        entries.append(f"[{date} | {author}] {text}")

    return "\n\n".join(entries)


def _generate() -> dict:
    dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
    profile = {
        "customer_id":      str(uuid.uuid4()),
        "full_name":        fake.name(),
        "email":            fake.email(),
        "date_of_birth":    dob.isoformat(),
        "telephone_number": fake.phone_number(),
        "home_postcode":    fake.postcode(),
        "card_id":          fake.numerify("############"),
        "home_station":     fake.random_element(TFL_STATIONS),
    }
    profile["customer_notes"] = _generate_notes(profile)
    return profile


def main():
    catalog = _args.catalog
    schema = _args.schema
    table = f"{catalog}.{schema}.customer_profiles"
    now = datetime.now(timezone.utc)
    profiles = [_generate() for _ in range(PROFILE_COUNT)]
    rows = [
        {
            **p,
            "raw_payload":  json.dumps(p),
            "_inserted_at": now,
            "_updated_at":  now,
            "_delete_at":   now + timedelta(days=RETENTION_DAYS_CUSTOMER_PROFILE),
        }
        for p in profiles
    ]

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            raw_payload      STRING  COMMENT 'Verbatim synthetic profile record as JSON.',
            customer_id      STRING,
            full_name        STRING,
            email            STRING,
            date_of_birth    DATE,
            telephone_number STRING,
            home_postcode    STRING,
            card_id          STRING,
            home_station     STRING,
            customer_notes   STRING    COMMENT 'Free-text CRM notes entered by staff or the customer. May contain unstructured PII.',
            _inserted_at     TIMESTAMP COMMENT 'Platform: when this row first arrived in bronze. Immutable.',
            _updated_at      TIMESTAMP COMMENT 'Platform: when this row was last written.',
            _delete_at       TIMESTAMP COMMENT 'Platform: Auto TTL expiry. 2-year retention — customer profile data for the personalisation feature.'
        )
        COMMENT 'SYNTHETIC DATA — generated via Faker (en_GB). Represents hypothetical TfL contactless-card registrations. Not real customer data.'
    """)

    df = (
        spark.createDataFrame(rows)
        .withColumn("date_of_birth", F.col("date_of_birth").cast("date"))
        .withColumn("_inserted_at",  F.col("_inserted_at").cast("timestamp"))
        .withColumn("_updated_at",   F.col("_updated_at").cast("timestamp"))
        .withColumn("_delete_at",    F.col("_delete_at").cast("timestamp"))
    )

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"Wrote {PROFILE_COUNT} synthetic profiles to {table}")


main()
