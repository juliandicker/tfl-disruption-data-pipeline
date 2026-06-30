-- Set per-table freshness SLAs as required by the platform governance standard.
-- The platform-governance-daily job reads platform.freshness_sla from each table's
-- TBLPROPERTIES and uses it to compute sla_status in admin.shared.retention_compliance.
-- Idempotent: SET TBLPROPERTIES overwrites existing values. Safe to re-run.
--
-- SLA rationale:
--   tfl_arrivals       30m — real-time operational data; pipeline runs every 15m
--   customer_profiles  1h  — synthetic, regenerated each run but not time-critical
--   customer_journeys  1h  — one pipeline stage behind bronze
--   disruption_summary 1h  — end of the pipeline chain
--   notification_targets 1h — end of the pipeline chain

ALTER TABLE bronze.tfl.tfl_arrivals
SET TBLPROPERTIES ('platform.freshness_sla' = '30m');

ALTER TABLE bronze.tfl.customer_profiles
SET TBLPROPERTIES ('platform.freshness_sla' = '1h');

ALTER TABLE silver.tfl.customer_journeys
SET TBLPROPERTIES ('platform.freshness_sla' = '1h');

ALTER TABLE gold.travel.disruption_summary
SET TBLPROPERTIES ('platform.freshness_sla' = '1h');

ALTER TABLE gold.travel.notification_targets
SET TBLPROPERTIES ('platform.freshness_sla' = '1h');
