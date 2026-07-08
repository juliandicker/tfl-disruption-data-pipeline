-- Bootstrap class.* governed tags on known PII columns.
-- Makes ABAC masking active immediately without waiting for the Data Classification scanner (~24h).
-- Idempotent: SET TAGS overwrites existing values. Safe to re-run.
-- Prerequisite: the travel pipeline SP must have ASSIGN on each class.* governed tag
--   (Catalog Explorer → Govern → Governed Tags → each tag → Permissions).
--
-- INTENTIONAL OMISSIONS — do not add these:
--   silver.tfl.customer_journeys.telephone_number
--   bronze.tfl.customer_profiles.telephone_number
--   bronze.tfl.customer_profiles.customer_notes
--
-- These columns are left untagged on purpose to demonstrate that the Databricks
-- agentic Data Classification engine detects and tags them automatically within ~24h,
-- including PII embedded in unstructured free-text (customer_notes). This showcases
-- what happens in practice when new PII fields are added without a governance update.

-- customer_journeys and notification_targets are Lakeflow streaming tables
-- (dlt.create_streaming_table) — plain ALTER TABLE ALTER COLUMN SET TAGS
-- is rejected against pipeline-managed objects; ALTER STREAMING TABLE is
-- required instead (confirmed).
ALTER STREAMING TABLE silver.tfl.customer_journeys  ALTER COLUMN full_name     SET TAGS ('class.name' = '');
ALTER STREAMING TABLE silver.tfl.customer_journeys  ALTER COLUMN email         SET TAGS ('class.email_address' = '');
ALTER STREAMING TABLE silver.tfl.customer_journeys  ALTER COLUMN date_of_birth SET TAGS ('class.date_of_birth' = '');
ALTER STREAMING TABLE silver.tfl.customer_journeys  ALTER COLUMN home_postcode SET TAGS ('class.location' = '');

ALTER STREAMING TABLE gold.travel.notification_targets ALTER COLUMN full_name  SET TAGS ('class.name' = '');
ALTER STREAMING TABLE gold.travel.notification_targets ALTER COLUMN email      SET TAGS ('class.email_address' = '');
