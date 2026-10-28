# Test Guide

## 1. Offline tests

Run from a clean checkout without downloading Toronto datasets or requiring a pre-existing warehouse:

```bash
python -m pytest -q \
  --ignore=tests/test_warehouse_load.py \
  --ignore=tests/integration/test_scd2_active_permits.py
```

Live tests are skipped unless explicitly enabled with `RUN_LIVE_TESTS=1`. Business-key checks skip when no extracted manifests are available.

## 2. Tests requiring local ingestion data

These checks require local snapshots, manifests, and/or a DuckDB warehouse from a successful ingestion run:

```bash
python -m pytest -q \
  tests/test_warehouse_load.py \
  tests/test_business_keys.py \
  tests/integration/test_scd2_active_permits.py
```

The Active Permits SCD2 integration test requires a captured snapshot under `data/raw/active_permits/`. The warehouse-validation test requires the expected manifests and `data/warehouse/toronto.duckdb`. The business-key tests skip if extracted manifests are unavailable.

## 3. Live/network-dependent tests

The fallback tests make requests to Toronto Open Data when enabled:

```bash
RUN_LIVE_TESTS=1 python -m pytest tests/test_live_fallbacks.py -q
```

The end-to-end setup test performs a live probe, a full Active Permits extraction, and a DuckDB load. Run it deliberately because it downloads real data:

```bash
RUN_LIVE_TESTS=1 python -m pytest tests/test_local_setup.py -q
```

## Dashboard theme and refresh metadata regression coverage

The theme CSS token checks, pure timestamp formatter, and optional Snowflake metadata read have targeted tests. Run them with:

```bash
python -m pytest -q tests/test_dashboard_theme_css.py tests/test_dashboard_refresh_metadata.py tests/test_dashboard_snowflake_backend.py
```

When reviewing a UI change, also manually switch the published dashboard between Light and Dark and verify that page labels, slider labels, metric cards, inputs, and select/multiselect text remain readable in both modes.

## Key SCD2 regression coverage

`tests/integration/test_dlt_scd2_row_hash.py` verifies that changing only the upstream `_id` does not create an extra SCD2 version, while changing a tracked attribute does. It uses synthetic Parquet snapshots and a temporary DuckDB database, so it does not require a captured Toronto snapshot.
