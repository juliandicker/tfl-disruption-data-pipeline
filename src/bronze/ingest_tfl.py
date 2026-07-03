"""
TfL tube line status ingestion → bronze.tfl.tfl_arrivals

Fetches current status and disruption data from the TfL Open Data API and writes
one row per station-line combination per ingestion run. The verbatim API response
is preserved in raw_payload alongside the parsed columns.

TfL API key is optional (set TFL_APP_KEY env var for higher rate limits).
"""

import argparse
import json
import os
from datetime import datetime, timedelta, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

TFL_BASE = "https://api.tfl.gov.uk"
APP_KEY = os.getenv("TFL_APP_KEY", "")

_parser = argparse.ArgumentParser()
_parser.add_argument("--catalog", default="bronze")
_parser.add_argument("--schema", default="tfl")
_args, _ = _parser.parse_known_args()

# Maps each real TfL station name to the tube lines that serve it.
# Keeps station names in sync with the list used by generate_profiles.py.
STATION_LINES: dict[str, list[str]] = {
    "Baker Street":             ["bakerloo", "circle", "hammersmith-city", "jubilee", "metropolitan"],
    "Bank":                     ["central", "northern", "waterloo-city"],
    "Barbican":                 ["circle", "hammersmith-city", "metropolitan"],
    "Bermondsey":               ["jubilee"],
    "Bethnal Green":            ["central"],
    "Bond Street":              ["central", "jubilee", "elizabeth"],
    "Borough":                  ["northern"],
    "Brixton":                  ["victoria"],
    "Canary Wharf":             ["jubilee", "elizabeth"],
    "Cannon Street":            ["circle", "district"],
    "Clapham Common":           ["northern"],
    "Clapham North":            ["northern"],
    "Clapham South":            ["northern"],
    "Covent Garden":            ["piccadilly"],
    "Earl's Court":             ["district", "piccadilly"],
    "Elephant & Castle":        ["bakerloo", "northern"],
    "Embankment":               ["bakerloo", "circle", "district", "northern"],
    "Euston":                   ["northern", "victoria"],
    "Farringdon":               ["circle", "hammersmith-city", "metropolitan", "elizabeth"],
    "Finsbury Park":            ["piccadilly", "victoria"],
    "Green Park":               ["jubilee", "piccadilly", "victoria"],
    "Hammersmith":              ["circle", "district", "hammersmith-city", "piccadilly"],
    "Highbury & Islington":     ["victoria"],
    "Highgate":                 ["northern"],
    "Holborn":                  ["central", "piccadilly"],
    "Hyde Park Corner":         ["piccadilly"],
    "Kennington":               ["northern"],
    "Kentish Town":             ["northern"],
    "Kilburn":                  ["jubilee"],
    "King's Cross St. Pancras": ["circle", "hammersmith-city", "metropolitan", "northern", "piccadilly", "victoria", "elizabeth"],
    "Knightsbridge":            ["piccadilly"],
    "Leicester Square":         ["northern", "piccadilly"],
    "Liverpool Street":         ["central", "circle", "hammersmith-city", "metropolitan", "elizabeth"],
    "London Bridge":            ["jubilee", "northern"],
    "Marble Arch":              ["central"],
    "Mile End":                 ["central", "district", "hammersmith-city"],
    "Moorgate":                 ["circle", "hammersmith-city", "metropolitan", "northern"],
    "Old Street":               ["northern"],
    "Oxford Circus":            ["bakerloo", "central", "victoria"],
    "Paddington":               ["bakerloo", "circle", "district", "hammersmith-city", "elizabeth"],
    "Pimlico":                  ["victoria"],
    "Putney Bridge":            ["district"],
    "Seven Sisters":            ["victoria"],
    "Shepherd's Bush":          ["central"],
    "Sloane Square":            ["circle", "district"],
    "Southwark":                ["jubilee"],
    "Stockwell":                ["northern", "victoria"],
    "Stratford":                ["central", "jubilee", "elizabeth"],
    "Temple":                   ["circle", "district"],
    "Tottenham Court Road":     ["central", "northern", "elizabeth"],
    "Tower Hill":               ["circle", "district"],
    "Vauxhall":                 ["victoria"],
    "Victoria":                 ["circle", "district", "victoria"],
    "Waterloo":                 ["bakerloo", "jubilee", "northern", "waterloo-city"],
    "Westminster":              ["circle", "district", "jubilee"],
}


def _get(path: str) -> list:
    params = {"app_key": APP_KEY} if APP_KEY else {}
    resp = requests.get(f"{TFL_BASE}{path}", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def main():
    catalog = _args.catalog
    schema = _args.schema
    table = f"{catalog}.{schema}.tfl_arrivals"
    ingested_at = datetime.now(timezone.utc)

    line_statuses = _get("/line/mode/tube/status")
    status_by_id = {line["id"]: line for line in line_statuses}

    rows = []
    for station_name, line_ids in STATION_LINES.items():
        for line_id in line_ids:
            line = status_by_id.get(line_id, {})
            statuses = line.get("lineStatuses", [{}])
            primary = statuses[0] if statuses else {}
            disruption = primary.get("disruption") or {}
            affected_stops = [
                s.get("commonName", s.get("name", ""))
                for s in disruption.get("affectedStops", [])
            ]

            rows.append({
                "raw_payload": json.dumps(line),
                "line_id": line_id,
                "line_name": line.get("name", line_id),
                "station_name": station_name,
                "status_severity": int(primary.get("statusSeverity", -1)),
                "status_severity_description": primary.get("statusSeverityDescription", "Unknown"),
                "disruption_reason": primary.get("reason", ""),
                "disruption_description": disruption.get("description", ""),
                "affected_stops_json": json.dumps(affected_stops),
                "ingested_at": ingested_at,
                "_inserted_at": ingested_at,
                "_updated_at": ingested_at,
                "_delete_at": ingested_at + timedelta(days=730),
            })

    if not rows:
        print("No rows to write — TfL API returned no line data.")
        return

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            raw_payload                 STRING    COMMENT 'Verbatim TfL API line response as JSON.',
            line_id                     STRING,
            line_name                   STRING,
            station_name                STRING,
            status_severity             INT       COMMENT '10 = Good Service; lower values indicate disruption.',
            status_severity_description STRING,
            disruption_reason           STRING,
            disruption_description      STRING,
            affected_stops_json         STRING    COMMENT 'JSON array of stop names affected by the disruption.',
            ingested_at                 TIMESTAMP,
            _inserted_at                TIMESTAMP COMMENT 'Platform: when this row first arrived in bronze. Immutable.',
            _updated_at                 TIMESTAMP COMMENT 'Platform: when this row was last written.',
            _delete_at                  TIMESTAMP COMMENT 'Platform: Auto TTL expiry. Raw operational data — 2-year retention.'
        )
        CLUSTER BY (line_id, ingested_at)
        COMMENT 'TfL tube line status per station-line combination. Appended every 15 minutes.'
    """)

    # CREATE TABLE IF NOT EXISTS is a no-op against a table that already exists with an
    # older schema. Table ACLs disable automatic schema migration on append, so any
    # column added to the DDL above must also be backfilled onto the live table here.
    existing_cols = {f.name for f in spark.table(table).schema.fields}
    platform_columns = [
        ("_inserted_at", "TIMESTAMP", "Platform: when this row first arrived in bronze. Immutable."),
        ("_updated_at",  "TIMESTAMP", "Platform: when this row was last written."),
        ("_delete_at",   "TIMESTAMP", "Platform: Auto TTL expiry. Raw operational data — 2-year retention."),
    ]
    missing_cols = [
        f"{name} {dtype} COMMENT '{comment}'"
        for name, dtype, comment in platform_columns
        if name not in existing_cols
    ]
    if missing_cols:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(missing_cols)})")

    df = (
        spark.createDataFrame(rows)
        .withColumn("ingested_at",  F.col("ingested_at").cast("timestamp"))
        .withColumn("status_severity", F.col("status_severity").cast("int"))
        .withColumn("_inserted_at", F.col("_inserted_at").cast("timestamp"))
        .withColumn("_updated_at",  F.col("_updated_at").cast("timestamp"))
        .withColumn("_delete_at",   F.col("_delete_at").cast("timestamp"))
    )

    df.write.format("delta").mode("append").saveAsTable(table)
    print(f"Wrote {len(rows)} rows to {table} at {ingested_at.isoformat()}")


main()
