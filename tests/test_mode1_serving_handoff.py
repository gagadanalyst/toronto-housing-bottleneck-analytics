from __future__ import annotations

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
