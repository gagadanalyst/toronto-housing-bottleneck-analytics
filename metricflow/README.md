# MetricFlow semantic metrics

This directory contains the isolated open-source MetricFlow environment and the
runner that validates semantic definitions, executes metric queries, and writes
the dashboard's versioned result package. The definitions live beside the
existing dbt model contracts in `dbt/models/marts/schema.yml`, using dbt v2
model-embedded semantic YAML.

## What is implemented

- **Active semantic model:** `mart_active_permit_age`, with metrics for current
  logical Active revisions, age-eligible revisions, median current age, and
  approximate continuous P90 current age. Active age is right-censored elapsed
  time so far, not a completed processing duration.
- **Cleared semantic model:** `fct_cleared_permit_revision`, with the current
  logical Cleared revision count and median / approximate continuous P90 for
  application-to-issue, application-to-completion, and issue-to-completion.
  Duration metrics include records marked `Closed` and exclude negative values
  for each interval. Three interval-specific valid-record count metrics expose
  the denominator behind each grouped median and approximate P90.
- **Dimensions:** permit type and application year (through the application-date
  time dimension).
- **Six result groups:** Active overall, by permit type, and by year; Cleared
  overall, by permit type, and by year.
- **No duplicated warehouse transformations:** MetricFlow reuses the existing
  dbt semantic-model relations and generates metric-query SQL from their
  definitions.

## Workflow and publication

`.github/workflows/metricflow-metrics.yml` provides three paths:

1. **Pull requests:** restores the currently published `warehouse-latest`
   DuckDB asset to a disposable runner file, validates the semantic definitions,
   and executes the six sample query groups. It does not publish a release or
   modify production data.
2. **Successful Transformation DuckDB publication:** after a successful DuckDB job has
   published the new warehouse, the workflow verifies the source run and source
   commit, restores the published database to a disposable runner file, runs
   MetricFlow, and uploads `toronto-metricflow-results.json` to the persistent
   `metrics-latest` GitHub Release. If the Transformation workflow did not publish a new DuckDB `warehouse`, MetricFlow publication is skipped.
3. **Manual backfill:** `workflow_dispatch` accepts the ID of an already
   successful Transformation run that published DuckDB. The workflow rechecks that run
   and the warehouse provenance; it does not run another ingestion or warehouse
   load. Manual dispatch must target `main`.

The result package includes the six query outputs plus generation time and the
identity of the Transformation warehouse refresh used to calculate them. The release is
a separately versioned result artifact; it is not the DuckDB warehouse itself.

## Dashboard consumption and relationship to the other pages

The **Metrics & semantic layer** page is the fifth and final page in the Streamlit
application. It downloads the JSON asset from `metrics-latest` and presents the
precomputed metrics. Visitors can select among all published Active count / age
measures and all three Cleared interval medians / approximate P90s / valid-record
counts for the grouped permit-type and application-year views. Each interval has
its own valid Closed-revision denominator. The page caches the result package for
15 minutes; its refresh button clears the cache and re-fetches the release asset. The public Streamlit runtime does not run dbt or
MetricFlow, connect to a MetricFlow service, or install MetricFlow dependencies.

The four existing pages retain their established dbt serving-mart and dashboard
calculation paths; they have not been migrated to MetricFlow. The Story page now
uses the existing `mart_active_backlog_by_year_type` relation for a joint
application-year × permit-type view. The other three analytical pages continue
to use their established serving models:

- **The story** reads existing Active backlog and matched Closed lifecycle
  summary marts.
- **Active permits** reads `mart_active_permit_age` (the same relation used by
  the Active semantic model) but computes filter-responsive statistics in
  Pandas. Its P90 is exact; the MetricFlow page's DuckDB P90 is approximate.
- **Closed-permit timelines** reads `mart_cleared_lifecycle_by_year_type`, a
  grouped application-year / permit-type summary. MetricFlow instead calculates
  lifecycle measures from `fct_cleared_permit_revision` at logical revision
  grain. Their cohorts and aggregation levels differ, so their values are not
  interchangeable.
- **Data quality & definitions** reads the existing dbt data-quality outputs.

**Backend scope:** the current MetricFlow workflow and `metrics-latest` package
are DuckDB-only. The Metrics & semantic layer page consumes those published
DuckDB results regardless of the `DASHBOARD_BACKEND` selected for the other
warehouse-backed pages. Snowflake MetricFlow execution is not currently
implemented.

## Local run

Use a separate environment and a **disposable copy** of the published DuckDB
warehouse. The runner executes `dbt run --select dim_date` against the supplied
file, so do not point it at a production database or the downloaded release
asset itself. The MetricFlow environment does not alter production dbt or
Streamlit dependencies:

```bash
python3.14 -m venv .venv-metricflow
. .venv-metricflow/bin/activate
python -m pip install -r metricflow/requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml

cp /path/to/toronto.duckdb /tmp/toronto-metricflow.duckdb
export DBT_DUCKDB_PATH=/tmp/toronto-metricflow.duckdb

python metricflow/run_metrics.py \
  --database "$DBT_DUCKDB_PATH" \
  --trigger-run-id 123456789 \
  --source-commit 0123456789abcdef0123456789abcdef01234567 \
  --output /tmp/toronto-metricflow-results
```

Replace the sample run ID and commit with the successful Transformation run identity
recorded in `metadata.dashboard_refresh_metadata`. When both arguments are
supplied, the runner checks them against the warehouse metadata before querying.
They must be supplied together. Pull-request validation omits both arguments;
the workflow restores the published warehouse to its own disposable file.

The runner executes `dbt deps`, rebuilds only the `dim_date` time spine, runs
`dbt parse`, `mf validate-configs`, `mf list metrics`, and the six configured
`mf query` commands before writing
`toronto-metricflow-results.json`.

DuckDB executes continuous P90 metrics through its approximate quantile
function. The metric descriptions and dashboard labels identify those values as
approximate; median metrics remain separate.

## Dependency isolation

The MetricFlow environment is intentionally separate from `dbt/requirements.txt`
and `dashboard/requirements.txt`. `dbt-metricflow[dbt-duckdb]` installs its own
compatible self-hosted dbt Core / adapter runtime (1.12-compatible); the current
model-embedded semantic YAML specification is supported by dbt v1.12 and dbt
v2. Production Mode 1 and the Streamlit dashboard continue using their existing
environments.
