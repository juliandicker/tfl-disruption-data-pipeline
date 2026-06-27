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

**Synthetic traveller profiles** — generated via `Faker`. Fields: `customer_id`, `full_name`, `email`, `date_of_birth`, `home_postcode`, `card_id`, `home_station`. Joined to TfL data on `home_station` for a "personalised disruption alert" use case. Always labelled as synthetic, explicitly and prominently — never blur this line.

## Pipeline architecture

### Why there is no landing layer

Both sources are typed, structured responses (API JSON + generator output) — not unknown files. Replay/audit value is preserved instead via a `raw_payload` column on each bronze table (verbatim response alongside parsed columns). Landing earns its place for file-push sources or unstructured capture; neither applies here.

### Bronze — Lakeflow Job (plain Python tasks)

Both sources write directly to bronze. A plain Job is right here — this is an HTTP call and a generator run on a schedule, not a transform that benefits from declarative orchestration.

| Table | Contents |
|---|---|
| `bronze.default.tfl_arrivals` | `raw_payload`, parsed fields, ingestion timestamp |
| `bronze.default.customer_profiles` | `raw_payload`, parsed fields, ingestion timestamp |

**Zero group grants on bronze.** Only the ingestion job's service principal reads/writes it. A negative test must confirm neither reader group can query bronze.

Liquid clustering on `tfl_arrivals` (not `PARTITIONED BY` — that's the dated approach).

### Silver → Gold — Lakeflow Spark Declarative Pipelines

Use Declarative Pipelines (formerly DLT) for transforms: automatic orchestration, retry, lineage, and data quality expectations.

| Table | Contents | PII |
|---|---|---|
| `silver.default.customer_journeys` | Cleaned, deduped, `customer_profiles` joined to arrivals/disruption on `home_station` | Yes — `full_name`, `email`, `home_postcode` |
| `gold.default.disruption_summary` | Aggregated, no PII | No |
| `gold.default.notification_targets` | Actionable alert output | Yes — contains `email` (aggregation ≠ anonymisation) |

Liquid clustering on `customer_journeys`.

**Data quality expectations (minimum):**
- `customer_journeys`: valid email format, `home_station` not null, `date_of_birth` not in future and plausible
- `disruption_summary`: timestamps not null, line names match known reference list

### Governance

**Classification**: run Databricks agentic Data Classification on `silver` and `gold` — don't hand-tag PII columns, let the classifier demonstrate the capability.

**Entra groups:**

| Group | Access |
|---|---|
| `sg-dbplat-standard-readers` | Silver/gold, PII columns masked |
| `sg-dbplat-pii-readers` | Silver/gold, PII columns unmasked (ABAC `EXCEPT` group) |
| `sg-dbplat-data-stewards` | Full visibility, manages governed tags and ABAC policies |

**ABAC**: one policy per catalog (`silver`, `gold`) masking columns tagged as PII, for all principals except `sg-dbplat-pii-readers` and `sg-dbplat-data-stewards`.

**Open question — Entra group provisioning**: either (a) `azuread` Terraform provider added to the infra repo, or (b) created out-of-band and referenced by name here. Decide before scaffolding grants.

### Observability

Lakehouse Monitoring on `silver.default.customer_journeys` and `gold.default.disruption_summary` — freshness, drift, anomaly detection.

FinOps/cost dashboards are explicitly out of scope here — that's a platform concern belonging to the infra repo, not a pipeline-specific concern.

### Deferred / excluded (with rationale)

| Item | Reason |
|---|---|
| Change Data Feed / AUTO CDC | No genuinely evolving upstream records with synthetic single-generation data |
| Forecasting/ML | Possible future MLOps phase; not folded into this DE-focused repo |
| Lakeflow Connect | Built for SaaS/DB ingestion; source here is a REST API called directly |
| Lakeflow Designer | No-code tool for citizen developers; undercuts engineering depth here |
| Genie / AI-BI dashboard | Wanted, but use case undefined — decide once gold tables have real shape |

## Databricks infrastructure

Provisioned and managed by `C:\source\simple-databricks-deployment`. Do not modify infrastructure from this repo.

### Workspace

The workspace URL and pipeline SP application ID change after each infra rebuild. They are not hardcoded here — the data platform team updates the `DATABRICKS_HOST` and `AZURE_CLIENT_ID` GitHub Actions secrets in this repo automatically after each `terraform apply`.

| | |
|---|---|
| SKU | Premium (required for Unity Catalog) |
| Region | `uksouth` |

### Unity Catalog

```
Metastore (uksouth)
├── bronze  → schema: tfl
├── silver  → schema: tfl
└── gold    → schema: tfl
```

Storage account: `dbplatsimpleadls` (resource group: `dbplat-simple-rg`)
