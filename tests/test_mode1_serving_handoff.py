from __future__ import annotations
import subprocess
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]

def test_loading_does_not_build_or_dispatch_serving():
    workflow = (ROOT / ".github/workflows/mode1-incremental-loader.yml").read_text()
    assert "Loading SCD2 / Mode 1 Incremental Loader (Validated Staging)" in workflow
    assert "loading-latest" in workflow
    assert "Publish validated DuckDB loading assets" in workflow
    assert "dbt build" not in workflow
    assert "Dispatch the Snowflake serving-schema refresh" not in workflow

def test_transformation_owns_dbt_and_snowflake_serving():
    workflow = (ROOT / ".github/workflows/transformation-dbt-serving.yml").read_text()
    assert "loading-latest" in workflow
    assert 'bin/dbt" build --profiles-dir .' in workflow
    assert "warehouse-latest" in workflow
    assert "gh workflow run snowflake-serving-schema.yml" in workflow

def test_serving_workflow_verifies_loading_job():
    workflow = (ROOT / ".github/workflows/snowflake-serving-schema.yml").read_text()
    assert "snapshot_loader_run_id:" in workflow
    assert "Loading SCD2 / Mode 1 Incremental Loader (Validated Staging)" in workflow
    assert "Stage Mode 1 Snowflake changes" in workflow
    assert "raw_refresh_run_id" not in workflow

def test_snowflake_raw_promoter_uses_module_execution():
    workflow = (ROOT / ".github/workflows/mode1-incremental-loader.yml").read_text()
    assert "run: python -m scripts.loader_promote_snowflake_raw --validate-only" in workflow
    assert "run: python -m scripts.loader_promote_snowflake_raw\n" in workflow
    assert "python scripts/loader_promote_snowflake_raw.py" not in workflow

def test_snowflake_raw_promoter_is_importable_as_a_module():
    subprocess.run([sys.executable, "-c", "import scripts.loader_promote_snowflake_raw"], cwd=ROOT, check=True)

def test_loading_workflow_uses_the_parquet_asset_extractor():
    workflow = (ROOT / ".github/workflows/mode1-incremental-loader.yml").read_text()
    assert "python -m scripts.extract_latest_snapshot_assets" in workflow
    assert (ROOT / "scripts/extract_latest_snapshot_assets.py").is_file()

def test_dbt_source_database_follows_the_selected_backend():
    sources = (ROOT / "dbt/models/sources.yml").read_text()
    assert 'database: "{{ target.database }}"' in sources

def test_shared_restore_script_change_routes_through_loading_first():
    loading = (ROOT / ".github/workflows/mode1-incremental-loader.yml").read_text()
    transformation = (ROOT / ".github/workflows/transformation-dbt-serving.yml").read_text()

    assert "      - scripts/restore_duckdb_production.py" in loading

    route = transformation.split("upstream_prefixes = (", 1)[1].split(")", 1)[0]
    assert '"scripts/restore_duckdb_production.py",' in route
