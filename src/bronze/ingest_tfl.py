"""
TfL tube line status ingestion → bronze.default.tfl_arrivals

Fetches current status and disruption data from the TfL Open Data API and writes
one row per station-line combination per ingestion run. The verbatim API response
is preserved in raw_payload alongside the parsed columns.

TfL API key is optional (set TFL_APP_KEY env var for higher rate limits).
"""

import json
import os
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

TFL_BASE = "https://api.tfl.gov.uk"
APP_KEY = os.getenv("TFL_APP_KEY", "")

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
            })

    if not rows:
        print("No rows to write — TfL API returned no line data.")
        return

    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.default.tfl_arrivals (
            raw_payload                 STRING  COMMENT 'Verbatim TfL API line response as JSON.',
            line_id                     STRING,
            line_name                   STRING,
            station_name                STRING,
            status_severity             INT     COMMENT '10 = Good Service; lower values indicate disruption.',
            status_severity_description STRING,
            disruption_reason           STRING,
            disruption_description      STRING,
            affected_stops_json         STRING  COMMENT 'JSON array of stop names affected by the disruption.',
            ingested_at                 TIMESTAMP
        )
        CLUSTER BY (line_id, ingested_at)
        COMMENT 'TfL tube line status per station-line combination. Appended every 15 minutes.'
    """)

    df = (
        spark.createDataFrame(rows)
        .withColumn("ingested_at", F.col("ingested_at").cast("timestamp"))
        .withColumn("status_severity", F.col("status_severity").cast("int"))
    )

    df.write.format("delta").mode("append").saveAsTable("bronze.default.tfl_arrivals")
    print(f"Wrote {len(rows)} rows to bronze.default.tfl_arrivals at {ingested_at.isoformat()}")


main()
