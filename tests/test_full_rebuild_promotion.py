from __future__ import annotations

from pathlib import Path

import pytest

from scripts import promote_snowflake_snapshot_rebuild as snow_promote
from scripts import validate_full_rebuild_promotion as promotion

@pytest.fixture(autouse=True)
def _enable_snowflake_operations(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_OPERATIONS_ENABLED", "true")




def _report(backend: str):
    return {
        "backend": backend,
        "status": "passed",
        "mode": "rebuild",
        "history_scope": "full",
        "full_history": True,
        "explicit_promotion_required": True,
        "warehouse_input_staging": "not_used",
        "selection": {"from_date": None, "to_date": None, "limit": None},
        "snapshot_count_discovered": 1,
        "snapshot_count_selected": 1,
        "snapshots": [
            {
                "snapshot_id": "123:1",
                "archive_sha256": "a" * 64,
                "release_tag": "toronto-raw-snapshot-archive-2026-11",
                "asset_name": "toronto-raw-snapshot-bundle-123-1.zip",
            }
        ],
        "history_validation": {"status": "passed"},
    }




def _complete_duckdb_report():
    identity = {
        "snapshot_id": "123:1",
        "format_version": 1,
        "order_timestamp": 1793548800.0,
        "run_id": "123",
        "run_attempt": 1,
        "source_commit": "a" * 40,
        "archive_sha256": "b" * 64,
        "release_tag": "toronto-raw-snapshot-archive-2026-11",
        "asset_name": "toronto-raw-snapshot-bundle-123-1.zip",
    }

    sources = ("active_permits", "cleared_permits")
    metrics = {
        "status": "passed",
        "total": 2,
        "current": 1,
        "expired": 1,
        "export_rows": 2,
        "invalid_intervals": 0,
        "overlapping_intervals": 0,
        "duplicate_current_business_keys": 0,
        "null_business_keys": 0,
    }

    return {
        "backend": "duckdb",
        "status": "passed",
        "mode": "rebuild",
        "history_scope": "full",
        "full_history": True,
        "explicit_promotion_required": True,
        "warehouse_input_staging": "not_used",
        "history_scope_basis": "all_discoverable_complete_v1_v2_release_archives",
        "snapshot_count_discovered": 1,
        "snapshot_count_selected": 1,
        "snapshot_count_pending_before_load": 0,
        "selection": {
            "from_date": None,
            "to_date": None,
            "limit": None,
            "limit_semantics": "most_recent_n_then_replay_chronologically",
            "ordering": "extracted_at_utc_ascending",
            "limit_applied_before_retry_skips": True,
        },
        "sources": {
            source: {"status": "passed", "failed": [], "blocked": []}
            for source in sources
        },
        "history_validation": {
            "status": "passed",
            "sources": {
                source: dict(metrics)
                for source in sources
            },
        },
        "ledger_entries": [
            {
                **identity,
                "source_name": source,
                "status": "succeeded",
            }
            for source in sources
        ],
        "snapshots": [dict(identity)],
    }


def test_full_rebuild_promotion_requires_all_scope_and_joint_fields(monkeypatch):
    duck, snow = _report("duckdb"), _report("snowflake")
    recomputed = {
        "status": "passed",
        "mode": "rebuild",
        "history_scope": "full",
        "full_history": True,
        "explicit_promotion_required": True,
        "promotion_performed": False,
        "snapshot_count": 1,
        "snapshot_count_discovered": 1,
        "snapshot_count_selected": 1,
        "snapshots": duck["snapshots"],
        "backends": {"duckdb": {}, "snowflake": {}},
        "archive_provenance": "passed",
        "backend_snapshot_coverage": "identical",
        "scd2_integrity": "passed",
    }
    monkeypatch.setattr(promotion, "validate_reports", lambda *_args: recomputed)
    saved = dict(recomputed)
    assert promotion.validate_full_rebuild(duck, snow, saved) == recomputed

    partial = _report("duckdb")
    partial["history_scope"] = "partial"
    with pytest.raises(ValueError, match="full Mode 2"):
        promotion.validate_full_rebuild(partial, snow, saved)


def test_full_rebuild_promotion_rejects_missing_joint_promotion_guard(monkeypatch):
    duck, snow = _report("duckdb"), _report("snowflake")
    recomputed = {
        "status": "passed",
        "mode": "rebuild",
        "history_scope": "full",
        "full_history": True,
        "explicit_promotion_required": True,
        "promotion_performed": False,
        "snapshot_count": 1,
        "snapshot_count_discovered": 1,
        "snapshot_count_selected": 1,
        "snapshots": duck["snapshots"],
        "backends": {"duckdb": {}, "snowflake": {}},
        "archive_provenance": "passed",
        "backend_snapshot_coverage": "identical",
        "scd2_integrity": "passed",
    }
    monkeypatch.setattr(promotion, "validate_reports", lambda *_args: recomputed)
    saved = dict(recomputed)
    saved["promotion_performed"] = True
    with pytest.raises(ValueError, match="promotion_performed"):
        promotion.validate_full_rebuild(duck, snow, saved)


def test_current_archive_verification_rejects_stale_candidate(monkeypatch, tmp_path):
    expected = {
        "snapshots": [
            {
                "snapshot_id": "one",
                "archive_sha256": "a",
                "release_tag": "tag",
                "asset_name": "asset.zip",
            }
        ]
    }
    monkeypatch.setattr(
        promotion,
        "discover_archives",
        lambda *_args, **_kwargs: [
            type(
                "A",
                (),
                {
                    "snapshot_id": "two",
                    "archive_sha256": "b",
                    "release_tag": "tag",
                    "asset_name": "new.zip",
                },
            )()
        ],
    )
    with pytest.raises(ValueError, match="archive set changed"):
        promotion.verify_current_archives(
            expected, repository="owner/repo", archive_dir=tmp_path
        )


def test_snowflake_mode2_promoter_requires_run_scoped_schema_and_confirmation(
    monkeypatch,
):
    monkeypatch.setattr(
        snow_promote,
        "_connection",
        lambda _uri: (_ for _ in ()).throw(AssertionError("must not connect")),
    )
    with pytest.raises(ValueError, match="Unsafe Mode 2"):
        snow_promote.promote("unused", "RAW", "123", 1, confirmed=True)
    with pytest.raises(ValueError, match="does not belong"):
        snow_promote.promote(
            "unused", "TORONTO_SNAPSHOT_REPLAY_999", "123", 1, confirmed=True
        )
    with pytest.raises(PermissionError, match="confirmation"):
        snow_promote.promote(
            "unused", "TORONTO_SNAPSHOT_REPLAY_123", "123", 1, confirmed=False
        )


def test_promotion_workflow_validates_the_exact_source_run_attempt():
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github"
        / "workflows"
        / "promote-snapshot-rebuild.yml"
    ).read_text(encoding="utf-8")
    assert '"--attempt", attempt' in workflow
    assert '"attempt,name,status,conclusion,headBranch,event,jobs"' in workflow
    assert "selected source workflow attempt does not match" in workflow


def test_snowflake_promotion_job_can_dispatch_the_serving_workflow():
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github"
        / "workflows"
        / "promote-snapshot-rebuild.yml"
    ).read_text(encoding="utf-8")
    snowflake_job = workflow.split("  promote-snowflake:", 1)[1]
    assert "actions: write" in snowflake_job.split("    env:", 1)[0]
    assert "gh workflow run snowflake-serving-schema.yml" in snowflake_job

def test_duckdb_only_full_rebuild_validates_both_source_ledgers():
    report = _complete_duckdb_report()

    result = promotion.validate_duckdb_only(report)

    assert result["status"] == "passed"
    assert result["backend_validation_policy"] == "duckdb-only"
    assert result["validated_backends"] == ["duckdb"]
    assert result["cross_backend_parity"] == "not_applicable"
    assert result["backend_snapshot_coverage"] == "not_applicable"
    assert result["archive_provenance"] == "passed"
    assert result["scd2_integrity"] == "passed"
    assert result["history_scope"] == "full"
    assert result["source_ledger_entries"] == 2
    assert result["promotion_performed"] is False


def test_duckdb_only_rejects_partial_history():
    report = _complete_duckdb_report()
    report["selection"]["limit"] = 1

    with pytest.raises(ValueError, match="partial selection"):
        promotion.validate_duckdb_only(report)


def test_duckdb_only_rejects_incomplete_source_ledger():
    report = _complete_duckdb_report()
    report["ledger_entries"].pop()

    with pytest.raises(ValueError, match="ledger is incomplete"):
        promotion.validate_duckdb_only(report)


def test_duckdb_only_rejects_scd2_integrity_violations():
    report = _complete_duckdb_report()
    report["history_validation"]["sources"]["active_permits"][
        "overlapping_intervals"
    ] = 1

    with pytest.raises(ValueError, match="integrity violations"):
        promotion.validate_duckdb_only(report)


def test_snowflake_snapshot_promotion_requires_enabled_gate(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_OPERATIONS_ENABLED", "false")

    with pytest.raises(PermissionError, match="SNOWFLAKE_OPERATIONS_ENABLED"):
        snow_promote.promote(
            "unused",
            "TORONTO_SNAPSHOT_REPLAY_123",
            "123",
            1,
            confirmed=True,
            summary_path=None,
        )

def test_promotion_workflow_keeps_duckdb_and_snowflake_promotion_independent():
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github"
        / "workflows"
        / "promote-snapshot-rebuild.yml"
    ).read_text(encoding="utf-8")

    verify_job = workflow.split("  verify:", 1)[1].split("  promote-duckdb:", 1)[0]
    duckdb_job = workflow.split("  promote-duckdb:", 1)[1].split("  promote-snowflake:", 1)[0]
    snowflake_job = workflow.split("  promote-snowflake:", 1)[1].split("  deploy-dbt-docs:", 1)[0]

    assert 'backend_policy: ${{ steps.policy.outputs.backend_policy }}' in workflow
    assert 'echo "backend_policy=duckdb-only"' in verify_job
    assert 'echo "backend_policy=both"' in verify_job
    assert "validate_full_rebuild_promotion.py" in verify_job
    assert "needs: verify" in duckdb_job
    assert "SNOWFLAKE_OPERATIONS_ENABLED" not in duckdb_job
    assert "needs.verify.outputs.backend_policy == 'both'" in snowflake_job
    assert "vars.SNOWFLAKE_OPERATIONS_ENABLED == 'true'" in snowflake_job
