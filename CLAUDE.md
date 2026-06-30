# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo does

Databricks workspace assets for a TfL disruption data pipeline: ingestion jobs, Lakeflow Spark Declarative Pipelines, data quality expectations, ABAC governance, and Lakehouse Monitoring. Deployed via **Databricks Asset Bundles (DABs)** — not Terraform.

Infrastructure (workspace, catalogs, storage, Unity Catalog grants) lives in a separate repo: `C:\source\simple-databricks-deployment`. This split is intentional: Terraform owns the platform layer, DABs owns the workspace-asset layer.

## Common commands

```bash
databricks bundle validate          # check bundle config without deploying
databricks bundle run <job-name>    # trigger a job or pipeline run
databricks bundle destroy           # remove deployed assets
```

Deployment goes through GitHub Actions only (`push` to `master` → `.github/workflows/deploy.yml`). Do not run `databricks bundle deploy` locally.

## Data sources

**TfL live API** — real public data, no PII. Arrivals and disruption data per line/station.

**Synthetic traveller profiles** — generated via `Faker`. Fields: `customer_id`, `full_name`, `email`, `date_of_birth`, `telephone_number`, `home_postcode`, `card_id`, `home_station`, `customer_notes`. Joined to TfL data on `home_station` for a "personalised disruption alert" use case. Always labelled as synthetic, explicitly and prominently — never blur this line.

`customer_notes` is free-text CRM-style notes (2–5 timestamped entries per profile) that deliberately mix clean operational entries with entries embedding PII — to exercise unstructured PII detection by Data Classification.

## Pipeline architecture

### Why there is no landing layer

Both sources are typed, structured responses (API JSON + generator output) — not unknown files. Replay/audit value is preserved instead via a `raw_payload` column on each bronze table (verbatim response alongside parsed columns). Landing earns its place for file-push sources or unstructured capture; neither applies here.

### Bronze — Lakeflow Job (plain Python tasks)

Both sources write directly to bronze. A plain Job is right here — this is an HTTP call and a generator run on a schedule, not a transform that benefits from declarative orchestration.

| Table | Contents |
|---|---|
| `bronze.tfl.tfl_arrivals` | `raw_payload`, parsed fields, `ingested_at`, platform metadata columns |
| `bronze.tfl.customer_profiles` | `raw_payload`, `customer_id`, `full_name`, `email`, `date_of_birth`, `telephone_number`, `home_postcode`, `card_id`, `home_station`, `customer_notes`, `ingested_at`, platform metadata columns |

**Zero group grants on bronze.** Only the ingestion job's service principal reads/writes it. A negative test must confirm neither reader group can query bronze.

Liquid clustering on `tfl_arrivals` (not `PARTITIONED BY` — that's the dated approach).

### Silver → Gold — Lakeflow Spark Declarative Pipelines

Use Declarative Pipelines (formerly DLT) for transforms: automatic orchestration, retry, lineage, and data quality expectations.

| Table | Contents | PII |
|---|---|---|
| `silver.tfl.customer_journeys` | Cleaned, deduped, `customer_profiles` joined to arrivals/disruption on `home_station`. Includes `telephone_number`, `date_of_birth`, and derived `age` (whole years at ingest time). SCD Type 1. | Yes — `full_name`, `email`, `date_of_birth`, `telephone_number`, `home_postcode` |
| `gold.travel.disruption_summary` | Aggregated disruption counts by line and day, no PII | No |
| `gold.travel.notification_targets` | Actionable alert output, one row per customer per disrupted line. SCD Type 1. | Yes — `full_name`, `email` (aggregation ≠ anonymisation) |

Liquid clustering on `customer_journeys`.

**Data quality expectations (minimum):**
- `customer_journeys`: valid email format, `home_station` not null, `date_of_birth` not in future and plausible
- `disruption_summary`: timestamps not null, line names match known reference list

### Governance

**Classification**: Enable Databricks agentic Data Classification on `silver` and `gold` after the pipeline first populates the tables (Catalog Explorer → catalog → Details → Data Classification → Enable). The engine applies `class.*` system governed tags to PII columns automatically within ~24 h.

The governance bootstrap (`governance/tag_pii_columns.sql`) only covers the original structured PII columns (`full_name`, `email`, `date_of_birth`, `home_postcode`). The following columns are **intentionally left untagged** in the bootstrap:

- `silver.tfl.customer_journeys.telephone_number`
- `bronze.tfl.customer_profiles.telephone_number`
- `bronze.tfl.customer_profiles.customer_notes` (unstructured free-text, may contain embedded PII)

This is deliberate: the classifier should detect and tag them within ~24 h, demonstrating what happens when new PII fields are added without a manual governance update — including PII buried in free-text. Do not add these to the bootstrap.

**Entra groups** (provisioned by `simple-databricks-deployment` via Terraform + `azuread` provider):

| Group | Access |
|---|---|
| `sg-dbplat-standard-readers` | Silver/gold, PII columns masked |
| `sg-dbplat-pii-readers` | Silver/gold, PII columns unmasked (ABAC `EXCEPT` group) |
| `sg-dbplat-data-stewards` | Full visibility, manages governed tags and ABAC policies |

**ABAC**: four catalog-level policies per catalog (`silver`, `gold`), one per `class.*` tag type, each with a type-specific masking UDF. Principal exemptions are in the policy `EXCEPT` clause — not embedded in UDF logic.

| `class.*` tag | Columns | Mask |
|---|---|---|
| `class.name` | `full_name` | `***MASKED***` |
| `class.email_address` | `email` | Character-by-character `*`, preserving `@` and `.` |
| `class.date_of_birth` | `date_of_birth` | Generalised to year (`1990-01-01`) |
| `class.location` | `home_postcode` | Outward code only (`SO17`) |

`setup_abac.py` also bootstraps these tags directly onto the known PII columns at job run time so masking is active before the Data Classification scan completes. This requires the pipeline SP to have `ASSIGN` on each `class.*` tag — grant once in Catalog Explorer → Govern → Governed Tags → each tag → Permissions. (`databricks_grants` does not yet support `governed_tag` as a securable type in the Terraform provider.)

**Platform metadata columns** — every managed table carries `_inserted_at` (immutable first-insert timestamp), `_updated_at` (refreshed every write, drives freshness monitoring), and `_delete_at` (Auto TTL expiry). SCD1 streaming tables use `except_column_list=["_inserted_at"]` in `apply_changes()` to preserve the original insert timestamp across merges. Retention: 2 years for `tfl_arrivals` (raw operational, no PII); 7 years for all other tables.

**Freshness SLAs** — set as `platform.freshness_sla` table properties via `governance/set_freshness_slas.sql`, run as part of `travel-governance-bootstrap`. The platform's `compute_freshness_metrics` job reads these to populate `sla_status` in `admin.shared.retention_compliance`.

| Table | SLA |
|---|---|
| `bronze.tfl.tfl_arrivals` | `30m` |
| `bronze.tfl.customer_profiles` | `1d` |
| `silver.tfl.customer_journeys` | `1d` |
| `gold.travel.disruption_summary` | `1h` |
| `gold.travel.notification_targets` | `1d` |

### Observability

Lakehouse Monitoring on `silver.tfl.customer_journeys` and `gold.travel.disruption_summary` — freshness, drift, anomaly detection.

The `tfl-pipeline` job ends with two parallel tasks after `run_gold_pipeline`: `setup_monitors` (idempotent Lakehouse Monitor creation) and `refresh_dashboard` (republishes the *TfL Disruption Intelligence* AI/BI Lakeview dashboard via the native `dashboard_task` type).

FinOps/cost dashboards are explicitly out of scope here — that's a platform concern belonging to the infra repo, not a pipeline-specific concern.

### Deferred / excluded (with rationale)

| Item | Reason |
|---|---|
| Change Data Feed / AUTO CDC | No genuinely evolving upstream records with synthetic single-generation data |
| Forecasting/ML | Possible future MLOps phase; not folded into this DE-focused repo |
| Lakeflow Connect | Built for SaaS/DB ingestion; source here is a REST API called directly |
| Lakeflow Designer | No-code tool for citizen developers; undercuts engineering depth here |
| Genie | Space-based NL querying — possible addition once gold tables have enough history |

## Databricks infrastructure

Provisioned and managed by `C:\source\simple-databricks-deployment`. Do not modify infrastructure from this repo.

### Workspace

The workspace URL and pipeline SP application ID change after each infra rebuild. They are not hardcoded here — the data platform team updates the `DATABRICKS_HOST` and `AZURE_CLIENT_ID` GitHub Actions secrets in this repo automatically after each `terraform apply`.

| | |
|---|---|
| SKU | Trial (Premium features, 14-day window per workspace — see infra repo) |
| Region | `northeurope` |

### Unity Catalog

```
Metastore (uksouth)
├── bronze  → schema: tfl
├── silver  → schema: tfl
└── gold    → schema: travel
```

Storage account: `dbplatsimpleadls` (resource group: `dbplat-simple-rg`)
