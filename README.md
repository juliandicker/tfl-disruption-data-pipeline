# TfL Disruption Data Pipeline

A Databricks workspace asset bundle demonstrating a production-grade data engineering pipeline on Azure Databricks with Unity Catalog. Uses real TfL (Transport for London) open data alongside synthetic customer profiles to build a personalised disruption alerting use case — covering ingestion, transformation, data quality, PII governance, and observability.

## Synthetic data notice

Customer profile data in this pipeline is **entirely synthetic**, generated using the [Faker](https://faker.readthedocs.io/) library. It represents hypothetical TfL contactless-card registrations. No real customer information is used or implied at any point. This is stated explicitly here and in every table comment that touches profile data.

---

## Architecture overview

```
TfL Open Data API ──┐
                    ├─► bronze ──► silver (Declarative Pipeline) ──► gold ──► monitors
Faker generator ────┘                                                     └──► dashboard
```

All stages are orchestrated by a single `tfl-pipeline` Lakeflow Job that runs on a 15-minute schedule:

```
ingest_tfl ──┐
              ├──► run_silver_pipeline ──► run_gold_pipeline ──► setup_monitors
generate_profiles ──┘                                        └──► refresh_dashboard
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
| `bronze.tfl.tfl_arrivals` | TfL tube line status per station-line combination. `raw_payload` + parsed fields + platform metadata columns. Liquid-clustered on `(line_id, _inserted_at)`. |
| `bronze.tfl.customer_profiles` | Synthetic customer profiles. `raw_payload` + `customer_id`, `full_name`, `email`, `date_of_birth`, `telephone_number`, `home_postcode`, `card_id`, `home_station`, `customer_notes` + platform metadata columns. Overwritten each run. |

`customer_notes` is free-text CRM-style notes entered by staff or the customer across 2–5 timestamped entries per profile. Entries are a deliberate mix of clean operational notes and notes that embed PII (name, email, phone, address) — to exercise unstructured PII detection by Data Classification.

**Bronze access**: zero group grants. Only the pipeline service principal reads/writes bronze. Run `python tests/test_bronze_access.py` to assert this is enforced.

### Silver → Gold — Lakeflow Spark Declarative Pipelines

Two separate pipelines handle the transform layer. Declarative Pipelines (formerly DLT) earn their place here: automatic orchestration, retry, lineage, and data quality expectations are all valuable for transforms.

| Table | Contains PII | Description |
|---|---|---|
| `silver.tfl.customer_journeys` | Yes | Cleaned, deduplicated join of customer profiles to TfL disruptions on `home_station`. Includes `telephone_number`, `date_of_birth`, and a derived `age` column (whole years at ingest time). SCD Type 1 streaming table. |
| `gold.travel.disruption_summary` | No | Aggregated disruption counts by line and day. |
| `gold.travel.notification_targets` | Yes | One row per affected customer per disrupted line, ready for alerting. Contains `full_name` and `email` — aggregation does not equal anonymisation. SCD Type 1 streaming table. |

**Data quality expectations** (enforced in the pipeline event log):
- `customer_journeys`: valid email format, `home_station` not null, `date_of_birth` plausible (not in future, not before 1900).
- `disruption_summary`: `disruption_date` not null, `line_id` matches known TfL line reference list.

### Platform metadata columns

Every managed table carries three columns required by the data platform governance standard:

| Column | Type | Set when | Purpose |
|---|---|---|---|
| `_inserted_at` | `TIMESTAMP` | Bronze: first insert. Silver/gold: propagated from the bronze source row(s) | When the underlying data first entered the platform — not when a given layer's transform ran |
| `_updated_at` | `TIMESTAMP` | Bronze: every write. Silver/gold: propagated from the bronze source row(s) (oldest of any fan-in inputs) | Drives freshness SLA monitoring in `admin.shared.retention_compliance` — reflects the age of the *data*, so a bronze ingestion failure shows up as staleness in silver/gold too |
| `_delete_at` | `TIMESTAMP` | At insert time, per-table per purpose | Drives platform Auto TTL — rows are purged after this date |

Retention is set by *purpose*, not by pipeline layer — GDPR's storage-limitation principle (Art 5(1)(e)) ties retention to what the data is *for*, not which medallion layer it sits in:

| Purpose | Tables | Retention |
|---|---|---|
| Operational (no personal data) | `tfl_arrivals`, `disruption_summary` | 2 years |
| Customer profile / identity | `customer_profiles` (incl. `customer_notes`) | 2 years |
| Personalised alerting linkage | `customer_journeys`, `notification_targets` | 1 year |

See CLAUDE.md for the full rationale per purpose, including the known `customer_notes` row-level-retention limitation.

### Lakehouse Monitoring and dashboard refresh

Two tasks run in parallel after `run_gold_pipeline`:

**`setup_monitors`** creates time-series Lakehouse monitors on `silver.default.customer_journeys` and `gold.default.disruption_summary` using the Databricks SDK — idempotent, no manual steps required on environment rebuild.

**`refresh_dashboard`** republishes the *TfL Disruption Intelligence* Lakeview (AI/BI) dashboard using the native Databricks `dashboard_task` type, so the published snapshot reflects the freshest gold data immediately after each pipeline run.

---

## Governance

**Data classification**: Enable Databricks agentic Data Classification on `silver` and `gold` after the pipeline first populates the tables (Catalog Explorer → catalog → Details → Data Classification → Enable). The engine applies `class.*` system governed tags to PII columns automatically within ~24 h. Do not hand-tag columns manually.

**Entra groups** (provisioned by `simple-databricks-deployment`):

| Group | Access |
|---|---|
| `sg-dbplat-standard-readers` | Silver and gold — PII columns masked |
| `sg-dbplat-pii-readers` | Silver and gold — PII columns unmasked (ABAC `EXCEPT` group) |
| `sg-dbplat-data-stewards` | Full visibility, manages governed tags and ABAC policies |

**ABAC column masking**: `src/governance/setup_abac.py` creates four catalog-level ABAC policies per catalog, keyed off `class.*` system governed tags from Data Classification. Each policy uses a type-specific masking UDF:

| Column | `class.*` tag | Masked value (standard readers) |
|---|---|---|
| `full_name` | `class.name` | `***MASKED***` |
| `email` | `class.email_address` | `****@*******.***` (structure preserved) |
| `date_of_birth` | `class.date_of_birth` | Year only — e.g. `1990-01-01` |
| `home_postcode` | `class.location` | Outward code only — e.g. `SO17` |

`sg-dbplat-pii-readers` and `sg-dbplat-data-stewards` are exempt from all masking via the policy `EXCEPT` clause.

The `travel-governance-bootstrap` job must be run once after the pipeline first populates the tables. It has one task:

- **`tag_pii_columns`** — bootstraps `class.*` governed tags onto known PII columns so ABAC masking is active immediately, without waiting for Data Classification (~24h). `telephone_number` and `customer_notes` are intentionally omitted — left for Data Classification to detect automatically, demonstrating what happens when new PII fields are added without a manual governance update.

**Freshness SLAs** are not set via this bootstrap job — each table's `platform.freshness_sla` is defined directly alongside its own definition:

| Table | SLA | Rationale | Set via |
|---|---|---|---|
| `bronze.tfl.tfl_arrivals` | `30m` | Real-time operational data; pipeline runs every 15 min | `TBLPROPERTIES` in the `CREATE TABLE` DDL + a post-write `ALTER TABLE`, in `src/bronze/ingest_tfl.py` |
| `bronze.tfl.customer_profiles` | `1d` | Synthetic customer data; daily refresh is sufficient | Same pattern, in `src/bronze/generate_profiles.py` |
| `silver.tfl.customer_journeys` | `1d` | Customer-scoped; SLA matches source profiles | `table_properties=` in `src/pipeline/silver.py` |
| `gold.travel.disruption_summary` | `1h` | Operational; expected fresh after every pipeline run | `table_properties=` in `src/pipeline/gold.py` |
| `gold.travel.notification_targets` | `1d` | Customer-scoped; SLA matches source profiles | `table_properties=` in `src/pipeline/gold.py` |

Silver/gold tables are Lakeflow Declarative Pipeline-managed (streaming tables / materialized view) — Databricks rejects `SET TBLPROPERTIES` via `ALTER TABLE`/`ALTER STREAMING TABLE` against pipeline-managed objects (`STREAMING_TABLE_OPERATION_NOT_ALLOWED`), so their SLA can only be set in the pipeline definition, the same way `delta.enableChangeDataFeed` already is. Bronze tables need both the DDL clause *and* the post-write `ALTER TABLE` because `CREATE TABLE IF NOT EXISTS` only applies `TBLPROPERTIES` on first creation, and an `overwriteSchema` write isn't guaranteed to preserve existing table properties.

This requires a **one-time admin step**: grant the pipeline SP `ASSIGN` on `class.name`, `class.email_address`, `class.date_of_birth`, and `class.location` in Catalog Explorer → Govern → Governed Tags → each tag → Permissions.

---

## CI/CD

### Team model

This repo is maintained by the data engineering team. The data platform team maintains `simple-databricks-deployment` (Terraform/infra). The only handoff between teams is secrets: after each infra apply, the platform team's pipeline updates two GitHub environment secrets in this repo via a GitHub App.

### Authentication

OIDC workload identity federation — no secrets stored. The pipeline service principal (`sp-tfl-pipeline`) exchanges a GitHub-issued OIDC token for a Databricks token at runtime. The federated credential is fully managed by Terraform in `simple-databricks-deployment`.

### GitHub environment secrets (`Settings → Environments → dev`)

| Secret | Managed by | Notes |
|---|---|---|
| `AZURE_CLIENT_ID` | Data platform team | Application (client) ID of `sp-tfl-pipeline` — updated automatically after each infra rebuild |
| `DATABRICKS_HOST` | Data platform team | Workspace URL — updated automatically after each infra rebuild |
| `AZURE_TENANT_ID` | Data platform team | Static — does not change between rebuilds |

### Deploying

Pushes to `master` that touch `src/`, `resources/`, `databricks.yml`, or the workflow file trigger an automated deploy. No local deploy tooling — all deployments go through `.github/workflows/deploy.yml`.

### After a workspace rebuild

The platform team updates `AZURE_CLIENT_ID` and `DATABRICKS_HOST`. The data engineering team then re-runs the last deploy workflow from the GitHub Actions UI (or merges any pending change to trigger a fresh run). No YAML edits required.

### First deploy on a new workspace

```bash
# 1. Confirm platform team has updated AZURE_CLIENT_ID and DATABRICKS_HOST secrets
# 2. Push to master (or re-run last workflow) to deploy bundle assets
# 3. Grant pipeline SP ASSIGN on class.name, class.email_address, class.date_of_birth,
#    class.location in Catalog Explorer → Govern → Governed Tags (one-time admin step)
# 4. Enable Data Classification on silver and gold catalogs
#    (Catalog Explorer → catalog → Details → Data Classification → Enable)
# 5. Let the pipeline run at least once to populate the tables, then run the bootstrap:
databricks bundle run travel-governance-bootstrap
```

The `tfl-pipeline` job runs automatically on its 15-minute schedule from that point.

### Before tearing down / after rebuild — bronze seed data

Since teardown destroys the `dbplatsimpleadls` storage account along with the
catalogs, `bronze.tfl.tfl_arrivals` doesn't survive a rebuild — `tfl-pipeline`
would otherwise start from an empty table with only the live TfL API's current
snapshot. `tools/seed_data/` holds a one-off export / reload script pair that
captures the accumulated `tfl_arrivals` history into
`seed_data/bronze/tfl_arrivals.parquet` before teardown, and restores it after.

Both scripts run **locally** against the Databricks SQL Statement Execution
API (via `databricks-sdk`) using your own admin credentials — not as a
deployed bundle job, cluster job, or uploaded workspace file. This works
despite bronze's zero group grants because an admin identity has implicit
full privileges regardless of compute type.

`customer_profiles` is deliberately **not** seeded — it's single-generation
Faker output with no accumulated history worth preserving, and
`generate_profiles.py` fully overwrites it on every `tfl-pipeline` run anyway.
Just let that task run after rebuild to repopulate it with an equally
realistic fresh batch.

```bash
pip install databricks-sdk pandas pyarrow

# Before teardown — capture current tfl_arrivals contents
python tools/seed_data/export_bronze_seed.py --profile <cli-profile>
git add seed_data/ && git commit -m "Capture tfl_arrivals seed data before teardown"

# After rebuild + redeploy — restore it, before enabling the live schedule
python tools/seed_data/reload_bronze_seed.py --profile <cli-profile>
```

Reload shifts `_inserted_at`/`_updated_at` forward so the most recent captured
row lands at "now" (freshness SLA passes immediately) while preserving the
relative spread between rows. See the docstrings in both scripts for details.

---

## Common commands

```bash
databricks bundle validate                        # check bundle config without deploying
databricks bundle run tfl-pipeline                # trigger a manual end-to-end run
databricks bundle run travel-governance-bootstrap # re-apply PII tags and freshness SLAs
```

> Deployment (`databricks bundle deploy`) is handled exclusively by GitHub Actions on push to `master`.

---

## What is explicitly excluded and why

| Item | Reason |
|---|---|
| **Landing layer** | Both sources are typed, structured responses. `raw_payload` preserves the audit trail that a landing layer would otherwise justify. |
| **Change Data Feed / AUTO CDC** | No genuinely evolving upstream records. Synthetic profiles are single-generation; TfL API is polled fresh each run. |
| **Forecasting / ML** | Possible future MLOps phase. Not folded into this data engineering focused repo. |
| **Lakeflow Connect** | Built for SaaS/database ingestion. The source here is a REST API called directly — Connect adds no value. |
| **Lakeflow Designer** | No-code tool aimed at citizen developers. Using it here would undercut the hands-on engineering depth this project demonstrates. |
| **Genie** | Space-based natural language querying — possible future addition once the gold tables have enough history to make freeform exploration useful. |
| **FinOps / cost dashboards** | A core data platform capability, not a pipeline concern. Belongs in the infrastructure repo alongside system table configuration. |
