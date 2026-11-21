# MetricFlow semantic metrics

This directory contains the isolated open-source MetricFlow environment and the
small result-publication runner. The semantic definitions themselves live beside
the existing dbt mart contracts in `dbt/models/marts/schema.yml`, using the dbt v2
model-embedded YAML specification.

## Design

- Reuses the existing `mart_active_permit_age` and `fct_cleared_permit_revision`
  models; it does not replace dbt transformations or duplicate warehouse logic.
- Runs against the published DuckDB warehouse after a successful Mode 1 DuckDB job.
- Validates the dbt v2 semantic configuration and queries metrics using the `mf` CLI.
- Publishes one versioned JSON result package in the `metrics-latest` GitHub Release.
- Streamlit reads that package; it does not connect to a MetricFlow API or install
  MetricFlow dependencies.

## Local run

Create a separate environment, install the pinned package, and configure the
existing dbt DuckDB profile. The MetricFlow environment does not alter the dbt
production or Streamlit dependencies:

```bash
python3.14 -m venv .venv-metricflow
. .venv-metricflow/bin/activate
python -m pip install -r metricflow/requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml
export DBT_DUCKDB_PATH=/path/to/toronto.duckdb

python metricflow/run_metrics.py   --database "$DBT_DUCKDB_PATH"   --trigger-run-id 123456789   --source-commit 0123456789abcdef0123456789abcdef01234567   --output /tmp/toronto-metricflow-results
```

Replace the sample run ID and commit with the successful Mode 1 run identity
recorded in the selected warehouse. When both are supplied, the runner checks
them against `metadata.dashboard_refresh_metadata` before querying metrics.
GitHub pull-request validation omits these two arguments and validates the
currently published warehouse without publishing a result package.

The runner checks warehouse refresh metadata, rebuilds only the disposable
`dim_date` time spine, runs `dbt deps`, `dbt parse`, `mf validate-configs`, and
the selected metric queries before writing
`toronto-metricflow-results.json`.

DuckDB executes the continuous P90 metrics through its approximate quantile
function. Their metric descriptions and dashboard labels identify them as
approximate; median metrics remain separate.

The MetricFlow environment is intentionally separate from `dbt/requirements.txt`
and `dashboard/requirements.txt`. `dbt-metricflow[dbt-duckdb]` installs its own
compatible self-hosted dbt Core/adapter runtime (1.12-compatible); the current
model-embedded semantic YAML spec is supported by dbt v1.12 and dbt v2. The
production Mode 1 workflow keeps using the existing dbt v2 environment.
