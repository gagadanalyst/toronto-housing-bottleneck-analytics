# Toronto Housing Bottleneck Analytics — dbt Models

## Purpose

This directory contains the dbt analytics layer for Toronto Housing Bottleneck Analytics, a portfolio project using City of Toronto building-permit data to examine permit lifecycles, active-permit age, and source-data quality.

The dbt project transforms the raw Active and Cleared Permits tables published by the ingestion workflow into documented, tested analytical models.

The project uses dbt v2, DuckDB, SQL, and the `dbt-labs/dbt_utils` package.

## Architecture

The models follow three layers:

1. **Staging:** typed and normalized representations of the raw Active and Cleared Permits tables, retaining source-row lineage and SCD2 history.
2. **Facts and dimensions:** logical permit-revision facts, a date dimension, and a shared permit-type dimension.
3. **Serving marts:** lifecycle-duration summaries for Cleared Permits and an eligibility-aware age product for Active Permits.

### Models

| Model | Purpose |
|---|---|
| `stg_active_permits` | Types and normalizes Active Permits data while preserving source-row lineage and SCD2 intervals. |
| `stg_cleared_permits` | Types and normalizes Cleared Permits data while preserving source-row lineage and SCD2 intervals. |
| `fct_active_permit_revision` | Consolidates physical source history to logical permit-revision grain and evaluates attribute conflicts. |
| `fct_cleared_permit_revision` | Consolidates cleared records to logical permit-revision grain and applies lifecycle-duration publication rules. |
| `dim_date` | Provides a continuous date dimension covering the lifecycle dates used by the project. |
| `dim_permit_type` | Provides permit-type values for analytical joins. |
| `mart_active_permit_age` | Calculates eligible active-permit age as of a configured date. |
| `mart_cleared_lifecycle_by_year_type` | Summarizes lifecycle durations, eligible date pairs, and negative-duration exceptions by application year and permit type. |

Staging models are materialized as views. Marts are materialized as tables. Model materializations and static-analysis settings are defined in `dbt_project.yml`.

### Macros

The project includes adapter-dispatched macros for date differences and UTC date conversion, plus ordered distinct string aggregation. These keep reusable SQL logic centralized and make adapter-specific behavior explicit.

### Data quality and testing

The project includes singular SQL tests for:

- Source-to-staging row parity and typed-value conversions.
- SCD2 interval validity and non-overlap.
- Logical permit-revision key completeness and uniqueness.
- Source-row aggregation and conflict-publication rules.
- Lifecycle-duration eligibility and treatment of negative intervals.
- Active-permit age eligibility and uniqueness.
- Date continuity and lifecycle-date coverage.

Tests are organized alongside the models and grouped by the contract they verify. Existing tests should be extended where needed rather than duplicated.

## Data scope and interpretation

The ingestion workflow publishes two Parquet snapshots and a DuckDB warehouse through the repository's rolling `warehouse-latest` GitHub release.

The Parquet snapshots represent full-source Active and Cleared Permits data. The warehouse tables are scoped to records with `APPLICATION_DATE >= 2020-01-01`.

The dbt source definitions reference the warehouse's `toronto` catalog and the raw tables:

- `toronto.raw.raw_active_permits`
- `toronto.raw.raw_cleared_permits`

The DuckDB filename is intentionally kept as `toronto.duckdb`. Renaming the file changes the catalog name exposed by DuckDB and can break source references.

### Analytical limitations

- **Cleared Permits:** lifecycle duration measures describe observed cleared/closed outcomes, not the full population of permits at application time. Closure selection bias affects comparisons, and 2026 is a partial year.
- **Active Permits:** age is right censored because these permits have not necessarily completed their lifecycle. The as-of-date must be stated when publishing results.
- **Negative durations:** intervals with negative durations are retained as data-quality evidence but excluded from publishable duration statistics.
- **Conflicting attributes:** where critical values disagree across physical source rows, the models preserve conflict evidence and withhold affected analytical values according to the model contracts.
- **Causality:** observed differences describe the data; they do not establish the causes of permit delays.

## Prerequisites

- Python 3.14.
- dbt v2 CLI, pinned to version 2.0.5 in `requirements.txt`.
- DuckDB support bundled with dbt v2; a separate `dbt-duckdb` installation is not required.
- Git and `curl` to obtain the published warehouse.

The project dependency on `dbt_utils` is pinned to version `1.4.1` in `packages.yml`, with the resolved dependency recorded in `package-lock.yml`.

Official references:

- [Install dbt](https://docs.getdbt.com/docs/local/install-dbt)
- [DuckDB setup for dbt v2](https://docs.getdbt.com/docs/local/connect-data-platform/duckdb-setup)
- [dbt package dependencies](https://docs.getdbt.com/docs/build/packages)

## Clean-checkout setup

Run commands in the following steps from the root of the repository unless indicated otherwise.

### 1. Create an isolated dbt environment

```bash
python3.14 -m venv dbt/.venv
source dbt/.venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r dbt/requirements.txt
```

Verify that the intended CLI is active:

```bash
command -v dbt
dbt --version
```

The expected dbt version for this project setup is `2.0.5`. Avoid accidentally using a different system-wide installation.

### 2. Disable anonymous usage statistics

Set the opt-out variables in the current shell:

```bash
export DBT_SEND_ANONYMOUS_USAGE_STATS=false
export DO_NOT_TRACK=1
```

Keep telemetry disabled for local project work. If using a different dbt installation or shell, verify the opt-out behavior before relying on it.

### 3. Obtain the published ingestion warehouse

The `warehouse-latest` release is a rolling publication, not an immutable historical release. A clean checkout downloads the currently published warehouse.

```bash
mkdir -p data/warehouse

if [ ! -s data/warehouse/toronto.duckdb ]; then
  curl -fL \
    "https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/releases/download/warehouse-latest/toronto.duckdb" \
    -o data/warehouse/toronto.duckdb
fi

test -s data/warehouse/toronto.duckdb
```

For a repeatable review against an exact snapshot, record the download date and SHA-256 hash of the warehouse file.

### 4. Create a separate development database

Do not build dbt models directly into the canonical ingestion warehouse. dbt writes its model outputs to the configured database.

Create a separate development copy:

```bash
mkdir -p data/warehouse/dbt-dev

if [ ! -s data/warehouse/dbt-dev/toronto.duckdb ]; then
  cp data/warehouse/toronto.duckdb \
     data/warehouse/dbt-dev/toronto.duckdb
fi
```

Configure the absolute path for the current terminal:

```bash
export DBT_DUCKDB_PATH="$PWD/data/warehouse/dbt-dev/toronto.duckdb"
```

Verify the path:

```bash
printf 'DBT_DUCKDB_PATH=%s\n' "$DBT_DUCKDB_PATH"
test -s "$DBT_DUCKDB_PATH"
```

Both the canonical warehouse and the development database are local data artifacts and must remain outside version control.

### 5. Configure the local profile

The tracked `profiles.yml.example` contains a safe, environment-driven profile. Create an ignored, project-local `profiles.yml` from that example:

```bash
test -e dbt/profiles.yml || \
  cp dbt/profiles.yml.example dbt/profiles.yml
```

The profile should have this structure:

```yaml
toronto_housing_analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DBT_DUCKDB_PATH') }}"
      schema: analytics
      threads: 4
```

The profile name must match `profile: toronto_housing_analytics` in `dbt_project.yml`.

The actual profile, local user configuration, caches, and databases are intentionally excluded from Git. Only `profiles.yml.example` is tracked.

## Run and validate the project

Run the dbt commands from the project directory:

```bash
cd dbt
```

### Install the pinned package dependencies

```bash
dbt deps
```

The committed `package-lock.yml` records the resolved `dbt_utils` dependency. Do not commit the generated `dbt_packages/` directory.

### Check the profile and database connection

```bash
dbt debug
```

This validates the configured project and connection. A successful debug command does not build models.

### Parse the project

```bash
dbt parse
```

This parses project configuration, models, macros, and tests without executing the model transformations.

### Build models and run tests

```bash
dbt build
```

This executes the model build and applicable tests. It writes analytical model outputs to the configured development database, not the canonical ingestion warehouse.

### Generate model documentation

```bash
dbt docs generate
```

Model and column descriptions are maintained in the properties YAML files under `models/`. Generate the documentation after the relevant model contracts and descriptions have been reviewed.

## Most recent local validation

On October 17, 2026, the project was built successfully using dbt v2.0.5, `dbt_utils` 1.4.1, and a separate DuckDB development database.

The observed result was:

- 8 models processed.
- 40 tests processed.
- 48 total resources successful.

These results describe that particular local run and warehouse snapshot. CI validation and future source refreshes must be checked independently.

## Repository hygiene

Commit the SQL, YAML, Jinja, dependency declarations, package lock, profile example, and this README.

Do not commit:

- `profiles.yml` or local user configuration.
- `.env` files or credentials.
- The DuckDB warehouse or development databases.
- Generated `target/`, `logs/`, or `dbt_packages/` directories.
- Python virtual environments or other local caches.

The repository's root `.gitignore` excludes these local artifacts.

## Reviewer path

For a quick review of the analytics implementation:

1. Read `dbt_project.yml`, `packages.yml`, and `package-lock.yml`.
2. Follow `models/sources.yml` into the two staging models.
3. Review the two logical permit-revision facts and the dimensions.
4. Inspect the Active age and Cleared lifecycle marts.
5. Review the singular tests under `tests/` for their documented contracts.
6. Recreate the development setup and run `dbt debug`, `dbt parse`, and `dbt build`.

Public conclusions should be drawn from verified model outputs and the published dashboard, with the eligibility rules and analytical limitations above kept visible.