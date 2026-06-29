"""
ABAC governance setup — type-specific column masking via Databricks Data Classification

Run once via the governance-setup DAB job after the pipeline has populated tables.
Idempotent: CREATE OR REPLACE for functions/policies; SET TAGS is idempotent; DROP
MASK is wrapped in try/except for first-run safety.

What this does:
  1. Creates 4 type-specific masking UDFs in the shared admin catalog.
  2. Drops any old table-level column masks from the prior RBAC approach.
  3. Creates 4 catalog-level ABAC policies per catalog (one per class.* tag type),
     exempting sg-dbplat-pii-readers and sg-dbplat-data-stewards.
  4. Verifies bronze has no group grants.

class.* tags are applied by the Data Classification engine (~24 h after being
enabled on the catalog by the infra CI step). Masking activates automatically
once the scanner assigns the tags.

Prerequisites:
  - Entra groups must exist in Azure (out-of-band).
  - Pipeline must have run at least once so the tables exist.
  - The admin catalog (default: admin.shared) must exist and the SP must have
    ALL PRIVILEGES on it — provisioned by the infra Terraform repo.
  - The job's SP must have MANAGE on silver/gold catalogs.
  - Pass --pipeline-sp <application_id> so the SP is exempted from masking
    policies. Without this, DLT fails with ABAC_POLICIES_NOT_SUPPORTED when
    writing to the silver streaming table.
"""

import argparse

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

_parser = argparse.ArgumentParser()
_parser.add_argument("--silver-catalog", default="silver")
_parser.add_argument("--gold-catalog", default="gold")
_parser.add_argument("--schema", default="tfl")
_parser.add_argument("--gold-schema", default="travel")
_parser.add_argument("--admin-catalog", default="admin")
_parser.add_argument("--admin-schema", default="shared")
_parser.add_argument("--pipeline-sp", default="", help="Application ID of the pipeline SP — exempted from all masking policies so DLT can write to silver/gold without ABAC blocking it")
_args, _ = _parser.parse_known_args()

silver = _args.silver_catalog
gold = _args.gold_catalog
schema = _args.schema
gold_schema = _args.gold_schema
admin = _args.admin_catalog
admin_schema = _args.admin_schema
pipeline_sp = _args.pipeline_sp

_PII_READERS = "sg-dbplat-pii-readers"
_DATA_STEWARDS = "sg-dbplat-data-stewards"


def sql(statement: str) -> None:
    print(f"SQL: {statement.strip()[:120]}")
    spark.sql(statement)


def try_sql(statement: str, label: str) -> None:
    try:
        sql(statement)
    except Exception as e:
        print(f"{label} skipped: {e}")


# ---------------------------------------------------------------------------
# 1. Type-specific masking UDFs — defined once in the shared admin catalog.
#    Pure transformations only — no identity checks. Principal targeting is
#    handled by the EXCEPT clause in each ABAC policy (step 4).
#    Policies on silver/gold reference these by their 3-part name so adding
#    a new data catalog requires no UDF duplication.
# ---------------------------------------------------------------------------

# full_name → opaque placeholder
sql(f"""
    CREATE OR REPLACE FUNCTION {admin}.{admin_schema}.mask_name(val STRING)
    RETURNS STRING
    RETURN '***MASKED***'
""")

# email → structure preserved, every non-delimiter character replaced
# e.g. john.doe@example.com → ****.***@*******.***
sql(f"""
    CREATE OR REPLACE FUNCTION {admin}.{admin_schema}.mask_email(val STRING)
    RETURNS STRING
    RETURN REGEXP_REPLACE(val, '[^@.]', '*')
""")

# date_of_birth → generalise to year (Jan 1 of birth year)
# Returns DATE so the type matches the column — e.g. 1990-07-15 → 1990-01-01
sql(f"""
    CREATE OR REPLACE FUNCTION {admin}.{admin_schema}.mask_dob(val DATE)
    RETURNS DATE
    RETURN MAKE_DATE(YEAR(val), 1, 1)
""")

# home_postcode → outward code only (UK inward code is always 3 chars,
# so strip spaces then drop the last 3 — works with or without a space)
# e.g. "SO17 1BJ" → "SO17", "SO171BJ" → "SO17"
sql(f"""
    CREATE OR REPLACE FUNCTION {admin}.{admin_schema}.mask_location(val STRING)
    RETURNS STRING
    RETURN LEFT(REPLACE(TRIM(val), ' ', ''), LENGTH(REPLACE(TRIM(val), ' ', '')) - 3)
""")

# ---------------------------------------------------------------------------
# 3. Remove old table-level column masks (prior RBAC approach)
#    Wrapped in try/except: will be a no-op if masks were never applied or
#    have already been removed.
# ---------------------------------------------------------------------------

_OLD_MASKS = [
    (f"{silver}.{schema}.customer_journeys",  "full_name"),
    (f"{silver}.{schema}.customer_journeys",  "email"),
    (f"{silver}.{schema}.customer_journeys",  "home_postcode"),
    (f"{gold}.{gold_schema}.notification_targets", "full_name"),
    (f"{gold}.{gold_schema}.notification_targets", "email"),
]

for table, column in _OLD_MASKS:
    try_sql(
        f"ALTER TABLE {table} ALTER COLUMN {column} DROP MASK",
        f"DROP MASK {table}.{column}",
    )

# ---------------------------------------------------------------------------
# 4. Catalog-level ABAC policies — one per class.* tag type
#    Scope: entire catalog, so new PII tables/columns are covered automatically
#    once they receive the matching class.* tag.
# ---------------------------------------------------------------------------

_POLICIES = [
    ("mask_name_columns",     "mask_name",     "class.name",          "name_col"),
    ("mask_email_columns",    "mask_email",    "class.email_address", "email_col"),
    ("mask_dob_columns",      "mask_dob",      "class.date_of_birth", "dob_col"),
    ("mask_location_columns", "mask_location", "class.location",      "loc_col"),
]

_sp_except = f", `{pipeline_sp}`" if pipeline_sp else ""

for catalog in (silver, gold):
    for policy_name, fn_name, tag_key, alias in _POLICIES:
        sql(f"""
            CREATE OR REPLACE POLICY {policy_name}
            ON CATALOG {catalog}
            COLUMN MASK {admin}.{admin_schema}.{fn_name}
            TO `account users` EXCEPT `{_PII_READERS}`, `{_DATA_STEWARDS}`{_sp_except}
            FOR TABLES
            MATCH COLUMNS has_tag('{tag_key}') AS {alias}
            ON COLUMN {alias}
        """)

# ---------------------------------------------------------------------------
# 5. Verify bronze has no group grants (unchanged from prior version)
# ---------------------------------------------------------------------------

bronze_grants = spark.sql("SHOW GRANTS ON CATALOG bronze").collect()
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
