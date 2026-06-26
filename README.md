# TfL Disruption Data Pipeline

A Databricks workspace asset bundle demonstrating a production-grade data engineering pipeline on Azure Databricks with Unity Catalog. Uses real TfL (Transport for London) open data alongside synthetic customer profiles to build a personalised disruption alerting use case — covering ingestion, transformation, data quality, PII governance, and observability.

## Synthetic data notice

Customer profile data in this pipeline is **entirely synthetic**, generated using the [Faker](https://faker.readthedocs.io/) library. It represents hypothetical TfL contactless-card registrations. No real customer information is used or implied at any point. This is stated explicitly here and in every table comment that touches profile data.

---

## Architecture overview

```
TfL Open Data API ──┐
                    ├─► bronze (Lakeflow Job) ─► silver (Declarative Pipeline) ─► gold
Faker generator ────┘
```

### Why two repos, two tools?

| Layer | Repo | Tool |
|---|---|---|
| Infrastructure (workspace, storage, catalogs, network) | `simple-databricks-deployment` | Terraform |
| Workspace assets (jobs, pipelines, monitors) | this repo | Databricks Asset Bundles |

Terraform owns the platform layer. DABs owns everything deployed into that platform. This is a deliberate toolchain split, not an inconsistency: each tool does exactly what it is designed for. The infrastructure repo provisions the `bronze`, `silver`, and `gold` Unity Catalog catalogs; this repo writes data into them.

### Bronze — Lakeflow Job

Both sources write directly to bronze. There is no landing layer because:
- The TfL API and Faker generator both produce typed, structured responses — not unknown files that need staging.
- The one property a landing layer would have preserved (point-in-time replay of irreplaceable API captures) is kept instead via a `raw_payload` column on each bronze table — the verbatim API or generator response alongside the parsed columns.

A plain Lakeflow Job is right for this layer: it is a scheduled HTTP call and a generator run, not a transform that benefits from declarative orchestration.

| Table | Description |
|---|---|
| `bronze.default.tfl_arrivals` | TfL tube line status per station-line combination. `raw_payload` + parsed fields + `ingested_at`. Liquid-clustered on `(line_id, ingested_at)`. |
| `bronze.default.customer_profiles` | Synthetic customer profiles. `raw_payload` + parsed fields + `ingested_at`. Overwritten each run. |

**Bronze access**: zero group grants. Only the job's service principal reads/writes bronze. Run `python tests/test_bronze_access.py` to assert this is enforced.

### Silver → Gold — Lakeflow Spark Declarative Pipelines

Two separate pipelines (silver and gold) handle the transform layer. Declarative Pipelines (formerly DLT) earn their place here: automatic orchestration, retry, lineage, and data quality expectations are all valuable for transforms, unlike the straightforward ingestion step above.

| Table | Contains PII | Description |
|---|---|---|
| `silver.default.customer_journeys` | Yes | Cleaned, deduplicated join of customer profiles to TfL disruptions on `home_station`. |
| `gold.default.disruption_summary` | No | Aggregated disruption counts by line and day. |
| `gold.default.notification_targets` | Yes | One row per affected customer per disrupted line, ready for alerting. Contains `email` — aggregation does not equal anonymisation. |

**Data quality expectations** (enforced in the pipeline event log):
- `customer_journeys`: valid email format, `home_station` not null, `date_of_birth` plausible (not in future, not before 1900).
- `disruption_summary`: `disruption_date` not null, `line_id` matches known TfL line reference list.

---

## Governance

**Data classification**: Run Databricks agentic Data Classification on `silver` and `gold` after the pipeline populates the tables. The classifier will tag `full_name`, `email`, `date_of_birth`, and `home_postcode` automatically against its built-in PII taxonomy. Do not hand-tag these columns.

**Entra groups** (must be pre-created in Azure before running governance setup):

| Group | Access |
|---|---|
| `sg-dbplat-standard-readers` | Silver and gold — PII columns masked |
| `sg-dbplat-pii-readers` | Silver and gold — PII columns unmasked (ABAC `EXCEPT` group) |
| `sg-dbplat-data-stewards` | Full visibility, manages governed tags and ABAC policies |

**ABAC column masking**: `src/governance/setup_abac.py` creates a `pii_mask` function in both catalogs that returns the raw value for `sg-dbplat-pii-readers` and `sg-dbplat-data-stewards`, and `'***MASKED***'` for everyone else. The mask is applied to `full_name`, `email`, and `home_postcode` in `customer_journeys`, and `full_name` and `email` in `notification_targets`.

**Observability**: Lakehouse Monitoring is configured on `silver.default.customer_journeys` and `gold.default.disruption_summary` for freshness, drift, and anomaly detection.

---

## Workspace sync (after each spin-up)

The workspace URL and ID are **never hardcoded** in this repo — they change every time the demo workspace is torn down and rebuilt. After each `terraform apply` in `simple-databricks-deployment`:

```powershell
# 1. Update the Databricks CLI profile with the new workspace URL:
.\scripts\sync-workspace.ps1

# 2. Authenticate against the new workspace:
databricks auth login
```

That is all. No YAML edits required. `databricks.yml` uses `workspace.profile: DEFAULT` which reads from `~/.databrickscfg`.

---

## Deploy sequence

```powershell
# Validate bundle config (no deployment):
databricks bundle validate

# Deploy jobs, pipelines, and monitors:
databricks bundle deploy

# Populate bronze tables (first run):
databricks bundle run bronze-ingestion

# Run the silver pipeline (creates customer_journeys):
databricks bundle run silver-pipeline

# Run the gold pipeline (creates disruption_summary and notification_targets):
databricks bundle run gold-pipeline

# Re-deploy to activate Lakehouse Monitors (tables must exist):
databricks bundle deploy

# Apply ABAC masking policies and catalog grants (run once):
databricks bundle run governance-setup

# Verify bronze is blocked to reader groups:
python tests/test_bronze_access.py
```

Lakehouse Monitors are bundled in the DAB but require the target tables to exist at deploy time. The second `databricks bundle deploy` (after the pipelines have run) deploys them successfully.

---

## Common commands

```bash
databricks bundle validate          # check bundle config without deploying
databricks bundle deploy            # deploy/update all assets
databricks bundle run <job-name>    # trigger a job or pipeline run
databricks bundle destroy           # remove all deployed assets
```

---

## What is explicitly excluded and why

| Item | Reason |
|---|---|
| **Landing layer** | Both sources are typed, structured responses. `raw_payload` preserves the audit trail that a landing layer would otherwise justify. |
| **Change Data Feed / AUTO CDC** | No genuinely evolving upstream records. Synthetic profiles are single-generation; TfL API is polled fresh each run. Revisit if profile generation is extended to simulate updates. |
| **Forecasting / ML** | Possible future MLOps phase. Not folded into this data engineering focused repo. |
| **Lakeflow Connect** | Built for SaaS/database ingestion. The source here is a REST API called directly — Connect adds no value. |
| **Lakeflow Designer** | No-code tool aimed at citizen developers. Using it here would undercut the hands-on engineering depth this project demonstrates. |
| **Genie / AI-BI dashboard** | Wanted, but the use case is undefined. Revisit once `gold.disruption_summary` has real shape to query against. |
| **FinOps / cost dashboards** | A core data platform capability, not a pipeline concern. Belongs in the infrastructure repo alongside system table configuration. |
