-- Bootstrap class.* governed tags on known PII columns.
-- Makes ABAC masking active immediately without waiting for the Data Classification scanner (~24h).
-- Idempotent: SET TAGS overwrites existing values. Safe to re-run.
-- Prerequisite: the travel pipeline SP must have ASSIGN on each class.* governed tag
--   (Catalog Explorer → Govern → Governed Tags → each tag → Permissions).

ALTER TABLE silver.tfl.customer_journeys  ALTER COLUMN full_name     SET TAGS ('class.name' = '');
ALTER TABLE silver.tfl.customer_journeys  ALTER COLUMN email         SET TAGS ('class.email_address' = '');
ALTER TABLE silver.tfl.customer_journeys  ALTER COLUMN date_of_birth SET TAGS ('class.date_of_birth' = '');
ALTER TABLE silver.tfl.customer_journeys  ALTER COLUMN home_postcode SET TAGS ('class.location' = '');

ALTER TABLE gold.travel.notification_targets ALTER COLUMN full_name  SET TAGS ('class.name' = '');
ALTER TABLE gold.travel.notification_targets ALTER COLUMN email      SET TAGS ('class.email_address' = '');
