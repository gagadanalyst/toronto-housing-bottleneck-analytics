# Toronto Housing Bottleneck Analytics — Ingestion Component

This repository contains the ingestion component for a portfolio project examining Toronto building-permit lifecycles, active-permit aging, and data quality using City of Toronto Open Data.

## Initial scope

The configured ingestion scope contains exactly two datasets:

- [Active Permits](https://open.toronto.ca/dataset/building-permits-active-permits/) — the active population used to analyse permit aging.
- [Cleared Permits](https://open.toronto.ca/dataset/building-permits-cleared-permits/) — completed outcomes and lifecycle dates used to contextualize elapsed time.

The two populations directly support the initial lifecycle and aging questions while keeping extraction, data-quality review, and downstream analysis manageable.

Additional datasets were considered during discovery but are outside the initial ingestion scope. The [Dataset Selection decision record](docs/decisions/0001-dataset-scope.md) documents the research, rationale, risks, and criteria for reconsidering these candidates. Only Active and Cleared Permits are configured and processed by this ingestion component.


## How it works

- CKAN extraction prefers the DataStore API with paginated retrieval and supports CSV/JSON fallback resources.
- Full-source Parquet snapshots are retained locally; the warehouse cutoff does not filter raw snapshots.
- Active and Cleared warehouse loads use dlt SCD2 with the composite permit-revision key: `permit_num`, `revision_num`, and `permit_type`.
- The warehouse applies an `APPLICATION_DATE >= 2020-01-01` cutoff to both configured permit sources.
- The SCD2 row hash excludes the volatile upstream `_id`, so a source-ID change alone does not create a new version.
- Key-quality and freshness/row-count warnings remain observable. They do not automatically block the warehouse load.

## Requirements and installation

The package requires Python 3.14.x.

From the repository root:

```bash
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

The default configuration is `open_data_config.yml`. It configures DuckDB at `data/warehouse/toronto.duckdb` and enables loading for Active and Cleared Permits.

Run commands from the repository root so the default configuration path resolves correctly. The standard local workflow does not require manually setting `DLT_PROJECT_DIR`, `DLT_CONFIG_FOLDER`, or `DLT_DATA_DIR`. These are optional dlt runtime path variables; consult the [official dlt environment-variable reference](https://dlthub.com/docs/api_reference/dlt/common/known_env) before deliberately overriding their defaults.

## Run ingestion

First, run a small metadata/schema probe for Active Permits:

```bash
toronto-ingest --source active_permits --probe-only
```

This probes the live source; it is not a full data extraction.

To extract Active Permits without loading the warehouse:

```bash
toronto-ingest --source active_permits --extract-only
```

To perform a full extraction and warehouse load for Active Permits:

```bash
toronto-ingest --source active_permits
```

To process both configured sources:

```bash
toronto-ingest
```

Full extraction can download hundreds of thousands of source rows. Run it deliberately and ensure sufficient local disk space is available. The default outputs are stored under `data/`: raw snapshots, manifests, checkpoints, the run lock, and local DuckDB state. These local artifacts are ignored by Git and are not published automatically by running the command.

## Tests

Start with the offline test command:

```bash
python -m pytest -q \
  --ignore=tests/test_warehouse_load.py \
  --ignore=tests/integration/test_scd2_active_permits.py
```

Tests requiring captured snapshots, manifests, or a local warehouse are described separately. Live fallback tests and the end-to-end setup test require `RUN_LIVE_TESTS=1` and network access; the end-to-end setup test performs a full Active Permits extraction.

See [Test Guide](tests/README.md) for test categories, prerequisites, and commands. Test results should be reported only after the corresponding command has actually run.

## Download the latest published data

After the main-branch CI workflow successfully completes its data publication job, the latest data assets are available from the [`warehouse-latest` release](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/releases/tag/warehouse-latest).

- [Active Permits Parquet snapshot](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/releases/download/warehouse-latest/toronto-active-permits.parquet)
- [Cleared Permits Parquet snapshot](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/releases/download/warehouse-latest/toronto-cleared-permits.parquet)
- [DuckDB warehouse database](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/releases/download/warehouse-latest/toronto.duckdb)

The two Parquet files contain full-source snapshots. The `APPLICATION_DATE >= 2020-01-01` cutoff applies to warehouse loading, not to the raw Parquet snapshots. The DuckDB database contains the `raw_active_permits` and `raw_cleared_permits` tables, including the warehouse's SCD2 history.

These links refer to rolling data assets, not versioned historical snapshots. Successful publication runs replace the assets with the latest data. The separate GitHub Actions artifacts are intended for run-level troubleshooting and are retained for 14 days; GitHub provides artifact downloads as ZIP archives.

## Local artifacts and publication

`.gitignore` excludes local environment files, dlt state, ingestion data, DuckDB files, caches, coverage output, and build artifacts. Do not commit credentials or local datasets. The publication workflow makes the intended Parquet and DuckDB assets available without committing local working files to Git.
