-- Set per-table freshness SLAs as required by the platform governance standard.
-- The platform-governance-daily job reads platform.freshness_sla from each table's
-- TBLPROPERTIES and uses it to compute sla_status in admin.shared.retention_compliance.
-- Idempotent: SET TBLPROPERTIES overwrites existing values. Safe to re-run.
--
-- SLA rationale:
--   tfl_arrivals         30m — real-time operational data; pipeline runs every 15m
--   customer_profiles    1d  — synthetic customer data; daily refresh is sufficient
--   customer_journeys    1d  — customer-scoped; daily SLA matches source profiles
--   disruption_summary   1h  — operational disruption data; expected fresh each run
--   notification_targets 1d  — customer-scoped; daily SLA matches source profiles

ALTER TABLE bronze.tfl.tfl_arrivals
SET TBLPROPERTIES ('platform.freshness_sla' = '30m');

ALTER TABLE bronze.tfl.customer_profiles
SET TBLPROPERTIES ('platform.freshness_sla' = '1d');

-- customer_journeys and notification_targets are Lakeflow streaming tables
-- (dlt.create_streaming_table) — plain ALTER TABLE fails with
-- INVALID_TARGET_FOR_SET_TBLPROPERTIES_COMMAND; the engine requires
-- ALTER STREAMING TABLE for these two specifically.
ALTER STREAMING TABLE silver.tfl.customer_journeys
SET TBLPROPERTIES ('platform.freshness_sla' = '1d');

ALTER TABLE gold.travel.disruption_summary
SET TBLPROPERTIES ('platform.freshness_sla' = '1h');

ALTER STREAMING TABLE gold.travel.notification_targets
SET TBLPROPERTIES ('platform.freshness_sla' = '1d');
