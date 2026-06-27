"""
ABAC governance setup — column masking and Unity Catalog grants

Run once via the governance-setup DAB job after the pipeline has populated tables.
Idempotent: uses CREATE OR REPLACE for functions and IF NOT EXISTS guards where needed.

What this does:
  1. Creates PII masking functions in silver and gold catalogs.
  2. Applies column masks to PII columns in silver.tfl.customer_journeys
     and gold.tfl.notification_targets.
  3. Grants silver and gold catalog access to the reader Entra groups.

Prerequisites:
  - Entra groups must exist in Azure before this script runs (out-of-band).
  - The pipeline must have run at least once so the tables exist.
  - The job's service principal must have MANAGE privilege on silver/gold catalogs.
"""

import argparse

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

_parser = argparse.ArgumentParser()
_parser.add_argument("--silver-catalog", default="silver")
_parser.add_argument("--gold-catalog", default="gold")
_parser.add_argument("--schema", default="tfl")
_args, _ = _parser.parse_known_args()

silver_catalog = _args.silver_catalog
gold_catalog = _args.gold_catalog
schema = _args.schema


def sql(statement: str) -> None:
    print(f"SQL: {statement.strip()[:120]}")
    spark.sql(statement)


# ---------------------------------------------------------------------------
# 1. Column masking functions
#    Members of sg-dbplat-pii-readers or sg-dbplat-data-stewards see plain text.
#    All other principals see the masked value.
# ---------------------------------------------------------------------------

for catalog in (silver_catalog, gold_catalog):
    sql(f"""
        CREATE OR REPLACE FUNCTION {catalog}.{schema}.pii_mask(val STRING)
        RETURNS STRING
        RETURN IF(
            IS_ACCOUNT_GROUP_MEMBER('sg-dbplat-pii-readers')
            OR IS_ACCOUNT_GROUP_MEMBER('sg-dbplat-data-stewards'),
            val,
            '***MASKED***'
        )
    """)

# ---------------------------------------------------------------------------
# 2. Apply column masks to PII columns
# ---------------------------------------------------------------------------

SILVER_PII_COLS = ["full_name", "email", "home_postcode"]
for col in SILVER_PII_COLS:
    sql(f"""
        ALTER TABLE {silver_catalog}.{schema}.customer_journeys
        ALTER COLUMN {col}
        SET MASK {silver_catalog}.{schema}.pii_mask
    """)

GOLD_NOTIFICATION_PII_COLS = ["full_name", "email"]
for col in GOLD_NOTIFICATION_PII_COLS:
    sql(f"""
        ALTER TABLE {gold_catalog}.{schema}.notification_targets
        ALTER COLUMN {col}
        SET MASK {gold_catalog}.{schema}.pii_mask
    """)

# ---------------------------------------------------------------------------
# 3. Catalog grants for reader groups
#    Bronze intentionally receives zero group grants.
# ---------------------------------------------------------------------------

for catalog in (silver_catalog, gold_catalog):
    sql(f"""
        GRANT USE_CATALOG, USE_SCHEMA, SELECT
        ON CATALOG {catalog}
        TO `sg-dbplat-standard-readers`
    """)
    sql(f"""
        GRANT USE_CATALOG, USE_SCHEMA, SELECT
        ON CATALOG {catalog}
        TO `sg-dbplat-pii-readers`
    """)
    sql(f"""
        GRANT USE_CATALOG, USE_SCHEMA, SELECT, MODIFY
        ON CATALOG {catalog}
        TO `sg-dbplat-data-stewards`
    """)

# ---------------------------------------------------------------------------
# 4. Verify bronze has no group grants (negative assertion)
# ---------------------------------------------------------------------------

bronze_grants = spark.sql(
    "SHOW GRANTS ON CATALOG bronze"
).collect()

reader_groups = {"sg-dbplat-standard-readers", "sg-dbplat-pii-readers"}
bronze_principals = {row["Principal"] for row in bronze_grants}
unexpected = bronze_principals & reader_groups

if unexpected:
    raise RuntimeError(
        f"Bronze catalog has unexpected grants for reader groups: {unexpected}. "
        "Review and revoke immediately — bronze must be accessible only to the ingestion service principal."
    )

print("Bronze grant check passed — no reader group access on bronze.")
print("Governance setup complete.")
