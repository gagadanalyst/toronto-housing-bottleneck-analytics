from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_mode1_loader_dispatches_serving_with_its_run_id():
    workflow = (ROOT / ".github/workflows/mode1-incremental-loader.yml").read_text(
        encoding="utf-8"
    )

    assert "name: Mode 1 Incremental Loader (Validated Staging)" in workflow
    assert "gh workflow run snowflake-serving-schema.yml" in workflow
    assert '-f snapshot_loader_run_id="$GITHUB_RUN_ID"' in workflow


def test_serving_workflow_verifies_the_current_mode1_job():
    workflow = (ROOT / ".github/workflows/snowflake-serving-schema.yml").read_text(
        encoding="utf-8"
    )

    assert "snapshot_loader_run_id:" in workflow
    assert "Mode 1 Incremental Loader (Validated Staging)" in workflow
    assert "Stage Mode 1 Snowflake changes" in workflow
    assert "raw_refresh_run_id" not in workflow
    assert "Snowflake Canonical RAW Refresh" not in workflow


def test_snowflake_raw_promoter_uses_module_execution():
    workflow = (ROOT / ".github/workflows/mode1-incremental-loader.yml").read_text(
        encoding="utf-8"
    )

    assert (
        "run: python -m scripts.loader_promote_snowflake_raw --validate-only"
        in workflow
    )
    assert "run: python -m scripts.loader_promote_snowflake_raw\n" in workflow
    assert "python scripts/loader_promote_snowflake_raw.py" not in workflow


def test_snowflake_raw_promoter_is_importable_as_a_module():
    subprocess.run(
        [sys.executable, "-c", "import scripts.loader_promote_snowflake_raw"],
        cwd=ROOT,
        check=True,
    )


def test_mode1_workflow_uses_the_duckdb_publication_asset_extractor():
    workflow = (ROOT / ".github/workflows/mode1-incremental-loader.yml").read_text(
        encoding="utf-8"
    )

    assert "python -m scripts.extract_latest_snapshot_assets" in workflow
    assert "python scripts.extract_latest_snapshot_assets.py" not in workflow
    assert (ROOT / "scripts/extract_latest_snapshot_assets.py").is_file()


def test_dbt_source_database_follows_the_selected_backend():
    sources = (ROOT / "dbt/models/sources.yml").read_text(encoding="utf-8")

    assert 'database: "{{ target.database }}"' in sources
    assert "database: toronto" not in sources
