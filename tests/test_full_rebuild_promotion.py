from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import promote_duckdb_snapshot_rebuild as duck_promote
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
        "history_scope_basis": "versioned_inventory_verified_v1_v2_release_archives",
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
            "sources": {source: dict(metrics) for source in sources},
        },
        "source_exclusions": [],
        "ledger_entries": [
            {
                **identity,
                "source_name": source,
                "status": "succeeded",
                "warehouse_status": "succeeded",
                "quarantined_row_count": 0,
                "quarantined_bad_row_count": 0,
                "quarantined_business_key_count": 0,
                "quarantine_sha256": None,
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
        "quarantine_parity": "passed",
        "source_exclusion_parity": "passed",
    }
    monkeypatch.setattr(
        promotion,
        "validate_reports",
        lambda *_args, **_kwargs: recomputed,
    )
    saved = dict(recomputed)
    assert promotion.validate_full_rebuild(duck, snow, saved) == recomputed

    partial = _report("duckdb")
    partial["history_scope"] = "partial"
    with pytest.raises(ValueError, match="full Mode 2"):
        promotion.validate_full_rebuild(partial, snow, saved)


def test_full_rebuild_passes_source_policy_to_cross_backend_validation(monkeypatch):
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
        "quarantine_parity": "passed",
        "source_exclusion_parity": "passed",
    }
    observed = {}

    def capture_policy(*_args, **kwargs):
        observed.update(kwargs)
        return recomputed

    monkeypatch.setattr(promotion, "validate_reports", capture_policy)

    exclusion = {
        "snapshot_id": "123:1",
        "source_name": "active_permits",
        "archive_sha256": "a" * 64,
        "reason": "Transient CKAN source undercount",
    }
    policy = {("123:1", "active_permits"): exclusion}

    promotion.validate_full_rebuild(
        duck,
        snow,
        dict(recomputed),
        source_exclusions=policy,
    )

    assert observed["source_exclusions"] == policy


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
        "quarantine_parity": "passed",
        "source_exclusion_parity": "passed",
    }
    monkeypatch.setattr(
        promotion,
        "validate_reports",
        lambda *_args, **_kwargs: recomputed,
    )
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


def test_mode2_promotion_delegates_serving_to_transformation():
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github"
        / "workflows"
        / "promote-snapshot-rebuild.yml"
    ).read_text(encoding="utf-8")
    snowflake_job = workflow.split("  promote-snowflake:", 1)[1]
    assert "actions: read" in snowflake_job.split("    env:", 1)[0]
    assert "gh workflow run snowflake-serving-schema.yml" not in snowflake_job
    assert "loading-latest" in workflow
    assert "Publish validated DuckDB loading assets" in workflow
    assert "dbt build" not in workflow
    assert "deploy-dbt-docs" not in workflow


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


def test_duckdb_only_validates_policy_bound_source_exclusion():
    report = _complete_duckdb_report()
    active = next(
        item
        for item in report["ledger_entries"]
        if item["source_name"] == "active_permits"
    )
    active["warehouse_status"] = "excluded"

    exclusion = {
        "snapshot_id": "123:1",
        "source_name": "active_permits",
        "archive_sha256": "b" * 64,
        "reason": "Test source undercount",
    }
    report["source_exclusions"] = [exclusion]
    policy = {("123:1", "active_permits"): exclusion}

    with pytest.raises(ValueError, match="without a matching source policy entry"):
        promotion.validate_duckdb_only(report, source_exclusions={})

    result = promotion.validate_duckdb_only(
        report,
        source_exclusions=policy,
    )

    assert result["status"] == "passed"
    assert result["source_exclusion_validation"] == "passed"
    assert result["source_exclusions"] == [exclusion]


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


def test_promotion_workflow_is_duckdb_only():
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github"
        / "workflows"
        / "promote-snapshot-rebuild.yml"
    ).read_text(encoding="utf-8")

    active = workflow.split(
        "# BEGIN RETIRED HISTORICAL WORKFLOW", 1
    )[0]

    assert "  verify:" in active
    assert "  promote-duckdb:" in active
    assert "promote-snowflake:" not in active
    assert "backend_policy=both" not in active
    assert "SNOWFLAKE_OPERATIONS_ENABLED" not in active
    assert "python -m scripts.validate_full_rebuild_promotion" in active
    assert "needs: verify" in active

def test_duckdb_promotion_summary_requires_quarantine_counts(tmp_path):
    report = _complete_duckdb_report()
    report["candidate"] = (
        "data/recovery/snapshot-replay/duckdb/toronto-replay-123.duckdb"
    )
    path = tmp_path / "duckdb-summary.json"
    path.write_text(json.dumps(report), encoding="utf-8")

    _, expected = duck_promote._read_report(path, "123")
    assert expected["123:1|active_permits"]["quarantined_row_count"] == 0

    report["ledger_entries"][0].pop("quarantined_bad_row_count")
    path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(ValueError, match="lacks immutable provenance"):
        duck_promote._read_report(path, "123")


def test_snowflake_promotion_summary_requires_quarantine_counts(tmp_path):
    report = _report("snowflake")
    report.update(
        candidate="TORONTO.TORONTO_SNAPSHOT_REPLAY_123",
        snapshot_count_discovered=1,
        snapshot_count_selected=1,
    )
    report["source_exclusions"] = []
    report["ledger_entries"] = [
        {
            "snapshot_id": "123:1",
            "source_name": source,
            "status": "succeeded",
            "warehouse_status": "succeeded",
            "run_id": "123",
            "run_attempt": 1,
            "source_commit": "a" * 40,
            "release_tag": "toronto-raw-snapshot-archive-2026-11",
            "asset_name": "toronto-raw-snapshot-bundle-123-1.zip",
            "format_version": 1,
            "archive_sha256": "b" * 64,
            "order_timestamp": 1793548800.0,
            "quarantined_row_count": 0,
            "quarantined_bad_row_count": 0,
            "quarantined_business_key_count": 0,
            "quarantine_sha256": None,
        }
        for source in ("active_permits", "cleared_permits")
    ]
    path = tmp_path / "snowflake-summary.json"
    path.write_text(json.dumps(report), encoding="utf-8")

    expected = snow_promote._expected_ledger_from_summary(path, "123")
    assert expected["123:1|active_permits"]["quarantined_row_count"] == 0

    report["ledger_entries"][0].pop("quarantined_business_key_count")
    path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(ValueError, match="lacks immutable identity"):
        snow_promote._expected_ledger_from_summary(path, "123")
