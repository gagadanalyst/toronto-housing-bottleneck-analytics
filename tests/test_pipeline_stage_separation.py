from __future__ import annotations

from pathlib import Path

from scripts.workflow_push_routing import (
    TRANSFORMATION_UPSTREAM_PATHS,
    should_run_push,
)

ROOT = Path(__file__).resolve().parents[1]


def test_loading_is_independent_from_transformations():
    workflow = (ROOT / ".github/workflows/mode1-incremental-loader.yml").read_text()
    assert "Loading SCD2 / Mode 1 Incremental Loader (Validated Staging)" in workflow
    assert "loading-latest" in workflow
    assert "Publish validated DuckDB loading assets" in workflow
    assert "dbt build" not in workflow
    assert "dbt docs generate" not in workflow
    assert "scripts.publish_duckdb_recovery" not in workflow
    assert "Dispatch the Snowflake serving-schema refresh" not in workflow


def test_transformation_owns_duckdb_serving_and_snowflake_is_archived():
    workflow = (ROOT / ".github/workflows/transformation-dbt-serving.yml").read_text()
    archived = ROOT / "docs/retired-workflows/snowflake-serving-schema.yml"
    assert "loading-latest" in workflow
    assert 'bin/dbt" build --profiles-dir .' in workflow
    assert "warehouse-latest" in workflow
    assert "Publish the four verified DuckDB production assets" in workflow
    assert "scripts/publish_duckdb_recovery.py" in workflow
    assert workflow.index(
        "Publish the four verified DuckDB production assets"
    ) < workflow.index("Preserve the immutable DuckDB serving recovery checkpoint")
    active_lines = [
        line
        for line in workflow.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert not any(
        "gh workflow run snowflake-serving-schema.yml" in line for line in active_lines
    )
    assert not (ROOT / ".github/workflows/snowflake-serving-schema.yml").exists()
    assert archived.is_file()
    assert "BEGIN RETIRED HISTORICAL WORKFLOW" in archived.read_text()


def test_complete_pipeline_schedule_uses_toronto_timezone():
    workflow = (ROOT / ".github/workflows/complete-pipeline.yml").read_text()
    assert 'cron: "' in workflow
    assert 'timezone: "America/Toronto"' in workflow
    assert "gh workflow run v2-snapshot-archive.yml" in workflow


def test_metricflow_follows_transformation():
    workflow = (ROOT / ".github/workflows/metricflow-metrics.yml").read_text()
    assert "Transformation / dbt Serving Refresh" in workflow
    assert "Mode 1 Incremental Loader (Validated Staging)" not in workflow


def test_mode2_promotion_updates_loading_baseline_not_dashboard_warehouse():
    workflow = (ROOT / ".github/workflows/promote-snapshot-rebuild.yml").read_text()
    assert "--release-tag loading-latest" in workflow
    assert "Publish validated DuckDB loading assets" in workflow
    assert "publish_release_assets.py" not in workflow
    assert "publish_duckdb_recovery.py" not in workflow
    assert "dbt build" not in workflow
    active_lines = [
        line
        for line in workflow.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert not any(
        "gh workflow run snowflake-serving-schema.yml" in line for line in active_lines
    )
    assert "rebootstrap-loading-duckdb:" in workflow
    active = workflow.split("# BEGIN RETIRED HISTORICAL WORKFLOW", 1)[0]
    assert "rebootstrap-loading-snowflake:" not in active
    assert "rebootstrap_from_mode2=true" in active
    assert "loading_published:" in active
    assert "raw_promoted:" not in active


def test_mode2_replay_captures_loading_baseline_not_serving_baseline():
    workflow = (ROOT / ".github/workflows/snapshot-replay.yml").read_text()
    assert "--release-tag loading-latest" in workflow
    assert "--database-asset toronto-loading.duckdb" in workflow

    active = workflow.split("# BEGIN RETIRED HISTORICAL WORKFLOW", 1)[0]
    seed_job = active.split("  seed-loading-baseline:", 1)[1].split("  duckdb:", 1)[0]
    duckdb_job = active.split("  duckdb:", 1)[1]

    assert "contents: write" in seed_job
    assert "scripts.seed_loading_release" in seed_job
    assert "group: toronto-housing-data-publication" in seed_job
    assert "cancel-in-progress: false" in seed_job
    assert "queue: max" in seed_job
    assert "needs: seed-loading-baseline" in duckdb_job
    assert "contents: read" in duckdb_job
    assert "scripts.seed_loading_release" not in duckdb_job


def test_mode2_duckdb_replay_seeds_loading_baseline_before_capture():
    workflow = (ROOT / ".github/workflows/snapshot-replay.yml").read_text()

    active = workflow.split("# BEGIN RETIRED HISTORICAL WORKFLOW", 1)[0]
    seed_job = active.split("  seed-loading-baseline:", 1)[1].split("  duckdb:", 1)[0]
    duckdb_job = active.split("  duckdb:", 1)[1]

    assert "Seed or verify loading-latest before Mode 2 reconstruction" in seed_job
    assert workflow.index("  seed-loading-baseline:") < workflow.index("  duckdb:")
    assert "needs: seed-loading-baseline" in duckdb_job
    assert (
        "Capture the authoritative DuckDB baseline before Mode 2 reconstruction"
        in duckdb_job
    )


def test_mode2_promotion_verification_has_scoped_seed_permissions():
    workflow = (ROOT / ".github/workflows/promote-snapshot-rebuild.yml").read_text()
    verify_job = workflow.split("  verify:", 1)[1].split("  promote-duckdb:", 1)[0]
    permissions = verify_job.split("permissions:", 1)[1].split("steps:", 1)[0]
    assert "actions: read" in permissions
    assert "contents: write" in permissions
    assert "actions: write" not in permissions


def test_loading_rollback_backup_is_preserved_on_failure_or_cancellation():
    workflows = (
        ROOT / ".github/workflows/mode1-incremental-loader.yml",
        ROOT / ".github/workflows/promote-snapshot-rebuild.yml",
    )
    for path in workflows:
        workflow = path.read_text()
        assert "steps.publish_loading_assets.outcome == 'failure'" in workflow
        assert "steps.publish_loading_assets.outcome == 'cancelled'" in workflow
        assert (
            "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
            in workflow
        )
        assert "retention-days: 30" in workflow
        assert "if-no-files-found: ignore" in workflow


def test_snowflake_serving_workflow_is_archived_and_not_executable():
    transformation = (
        ROOT / ".github/workflows/transformation-dbt-serving.yml"
    ).read_text()
    active_serving = ROOT / ".github/workflows/snowflake-serving-schema.yml"
    archived_serving = ROOT / "docs/retired-workflows/snowflake-serving-schema.yml"

    assert not active_serving.exists()
    assert archived_serving.is_file()
    assert "BEGIN RETIRED HISTORICAL WORKFLOW" in archived_serving.read_text()
    assert "snapshot_rebuild_promotion_run_id" not in transformation
    assert "Promote validated full-history SCD2 rebuild" not in transformation
    active_lines = [
        line
        for line in transformation.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert not any(
        "gh workflow run snowflake-serving-schema.yml" in line for line in active_lines
    )


def test_transformation_routes_loading_changes_before_serving():
    workflow = (ROOT / ".github/workflows/transformation-dbt-serving.yml").read_text()

    assert "scripts/workflow_push_routing.py" in workflow

    loading_paths = (
        "scripts/prepare_duckdb_candidate.py",
        "scripts/snowflake_candidate_utils.py",
        "scripts/promote_duckdb_candidate.py",
        "scripts/loader_promote_snowflake_raw.py",
    )
    for path in loading_paths:
        assert path in TRANSFORMATION_UPSTREAM_PATHS, (
            f"Missing upstream exclusion: {path}"
        )
        assert not should_run_push("transformation", (path,))

    push_paths = workflow.split("    paths:\n", 1)[1].split("  workflow_dispatch:", 1)[
        0
    ]
    assert "- scripts/promote_snowflake_serving_candidate.py" not in push_paths
    assert "- scripts/prepare_snowflake_serving_candidate.py" not in push_paths
    assert "- .github/workflows/snowflake-serving-schema.yml" not in push_paths


def test_complete_pipeline_uses_repository_scoped_dispatch_token():
    workflow = (ROOT / ".github/workflows/complete-pipeline.yml").read_text()

    assert "GH_TOKEN: ${{ secrets.TORONTO_PIPELINE_DISPATCH_TOKEN }}" in workflow
    assert "GH_TOKEN: ${{ github.token }}" not in workflow
    assert "permissions: {}" in workflow
