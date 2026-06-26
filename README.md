# TfL Disruption Data Pipeline

A Databricks workspace asset bundle demonstrating a production-grade data engineering pipeline on Azure Databricks with Unity Catalog. Uses real TfL (Transport for London) open data alongside synthetic customer profiles to build a personalised disruption alerting use case — covering ingestion, transformation, data quality, PII governance, and observability.

## Synthetic data notice

Customer profile data in this pipeline is **entirely synthetic**, generated using the [Faker](https://faker.readthedocs.io/) library. It represents hypothetical TfL contactless-card registrations. No real customer information is used or implied at any point. This is stated explicitly here and in every table comment that touches profile data.

---

## Architecture overview

```
TfL Open Data API ──┐
                    ├─► bronze ──► silver (Declarative Pipeline) ──► gold ──► monitors
Faker generator ────┘
```

All stages are orchestrated by a single `tfl-pipeline` Lakeflow Job that runs on a 15-minute schedule:

```
ingest_tfl ──┐
              ├──► run_silver_pipeline ──► run_gold_pipeline ──► setup_monitors
generate_profiles ──┘
```

### Why two repos, two tools?

| Layer | Repo | Tool |
|---|---|---|
| Infrastructure (workspace, storage, catalogs, network) | `simple-databricks-deployment` | Terraform |
| Workspace assets (jobs, pipelines, monitors) | this repo | Databricks Asset Bundles |

Terraform owns the platform layer. DABs owns everything deployed into that platform. The infrastructure repo provisions the `bronze`, `silver`, and `gold` Unity Catalog catalogs; this repo writes data into them.

### Bronze — ingestion tasks

Both sources write directly to bronze. There is no landing layer because both the TfL API and Faker generator produce typed, structured responses. Point-in-time replay is preserved instead via a `raw_payload` column on each bronze table — the verbatim API or generator response alongside the parsed columns.

| Table | Description |
|---|---|
| `bronze.default.tfl_arrivals` | TfL tube line status per station-line combination. `raw_payload` + parsed fields + `ingested_at`. Liquid-clustered on `(line_id, ingested_at)`. |
| `bronze.default.customer_profiles` | Synthetic customer profiles. `raw_payload` + parsed fields + `ingested_at`. Overwritten each run. |

**Bronze access**: zero group grants. Only the pipeline service principal reads/writes bronze. Run `python tests/test_bronze_access.py` to assert this is enforced.

### Silver → Gold — Lakeflow Spark Declarative Pipelines

Two separate pipelines handle the transform layer. Declarative Pipelines (formerly DLT) earn their place here: automatic orchestration, retry, lineage, and data quality expectations are all valuable for transforms.

| Table | Contains PII | Description |
|---|---|---|
| `silver.default.customer_journeys` | Yes | Cleaned, deduplicated join of customer profiles to TfL disruptions on `home_station`. |
| `gold.default.disruption_summary` | No | Aggregated disruption counts by line and day. |
| `gold.default.notification_targets` | Yes | One row per affected customer per disrupted line, ready for alerting. Contains `email` — aggregation does not equal anonymisation. |

**Data quality expectations** (enforced in the pipeline event log):
- `customer_journeys`: valid email format, `home_station` not null, `date_of_birth` plausible (not in future, not before 1900).
- `disruption_summary`: `disruption_date` not null, `line_id` matches known TfL line reference list.

### Lakehouse Monitoring

`setup_monitors` runs as the final task in the pipeline job after gold completes. It creates time-series Lakehouse monitors on `silver.default.customer_journeys` and `gold.default.disruption_summary` using the Databricks SDK — idempotent, no manual steps required on environment rebuild.

---

## Governance

**Data classification**: Run Databricks agentic Data Classification on `silver` and `gold` after the pipeline populates the tables. The classifier will tag `full_name`, `email`, `date_of_birth`, and `home_postcode` automatically. Do not hand-tag these columns.

**Entra groups** (pre-created in Azure via `simple-databricks-deployment`):

| Group | Access |
|---|---|
| `sg-dbplat-standard-readers` | Silver and gold — PII columns masked |
| `sg-dbplat-pii-readers` | Silver and gold — PII columns unmasked (ABAC `EXCEPT` group) |
| `sg-dbplat-data-stewards` | Full visibility, manages governed tags and ABAC policies |

**ABAC column masking**: `src/governance/setup_abac.py` creates a `pii_mask` function in both catalogs that returns the raw value for `sg-dbplat-pii-readers` and `sg-dbplat-data-stewards`, and `'***MASKED***'` for everyone else. Applied to `full_name`, `email`, and `home_postcode` in `customer_journeys`, and `full_name` and `email` in `notification_targets`.

---

## CI/CD

Pushes to `master` that touch `src/`, `resources/`, `databricks.yml`, or the workflow file trigger an automated deploy via GitHub Actions.

Authentication uses **OIDC workload identity federation** — no secrets stored. The pipeline service principal (`sp-tfl-pipeline`) exchanges a GitHub-issued OIDC token for a Databricks token at runtime. The federated credential is fully managed by Terraform in `simple-databricks-deployment`.

Required GitHub environment secrets (`Settings → Environments → dev`):

| Secret | Value |
|---|---|
| `AZURE_CLIENT_ID` | Application (client) ID of `sp-tfl-pipeline` — from `terraform output pipeline_sp_application_id` |
| `AZURE_TENANT_ID` | Azure tenant (directory) ID |
| `DATABRICKS_HOST` | Workspace URL, e.g. `https://adb-xxxx.azuredatabricks.net` |

---

## Deploy sequence

### First-time setup

```powershell
# 1. After terraform apply in simple-databricks-deployment, sync the workspace URL:
.\scripts\sync-workspace.ps1   # in simple-databricks-deployment

# 2. Authenticate the CLI:
databricks auth login

# 3. Validate and deploy all bundle assets:
databricks bundle validate
databricks bundle deploy

# 4. Apply ABAC masking policies (one-off, re-run after policy changes):
databricks bundle run governance-setup
```

The `tfl-pipeline` job runs automatically on its 15-minute schedule from that point: bronze ingestion, silver pipeline, gold pipeline, and monitor creation all happen without further intervention.

### After workspace rebuild

The same four steps above. No manual edits to any YAML files.

---

## Common commands

```bash
databricks bundle validate          # check bundle config without deploying
databricks bundle deploy            # deploy/update all assets
databricks bundle run tfl-pipeline  # trigger a manual end-to-end run
databricks bundle run governance-setup  # re-apply ABAC policies
databricks bundle destroy           # remove all deployed assets
```

---

## What is explicitly excluded and why

| Item | Reason |
|---|---|
| **Landing layer** | Both sources are typed, structured responses. `raw_payload` preserves the audit trail that a landing layer would otherwise justify. |
| **Change Data Feed / AUTO CDC** | No genuinely evolving upstream records. Synthetic profiles are single-generation; TfL API is polled fresh each run. |
| **Forecasting / ML** | Possible future MLOps phase. Not folded into this data engineering focused repo. |
| **Lakeflow Connect** | Built for SaaS/database ingestion. The source here is a REST API called directly — Connect adds no value. |
| **Lakeflow Designer** | No-code tool aimed at citizen developers. Using it here would undercut the hands-on engineering depth this project demonstrates. |
| **Genie / AI-BI dashboard** | Wanted, but the use case is undefined. Revisit once `gold.disruption_summary` has real shape to query against. |
| **FinOps / cost dashboards** | A core data platform capability, not a pipeline concern. Belongs in the infrastructure repo alongside system table configuration. |
