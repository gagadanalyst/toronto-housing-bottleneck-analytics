from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import promote_duckdb_snapshot_rebuild as promote


def _report(tmp_path: Path, run_id: str = "123") -> Path:
    candidate = (
        Path("data/recovery/snapshot-replay/duckdb") / f"toronto-replay-{run_id}.duckdb"
    )
    value = {
        "backend": "duckdb",
        "status": "passed",
        "mode": "rebuild",
        "history_scope": "full",
        "full_history": True,
        "explicit_promotion_required": True,
        "warehouse_input_staging": "not_used",
        "selection": {"from_date": None, "to_date": None, "limit": None},
        "snapshot_count_discovered": 1,
        "snapshot_count_selected": 1,
        "candidate": str(candidate),
        "snapshots": [{"snapshot_id": "123:1", "run_id": run_id}],
        "source_exclusions": [],
        "ledger_entries": [
            {
                "snapshot_id": "123:1",
                "source_name": source,
                "status": "succeeded",
                "warehouse_status": "succeeded",
                "format_version": 1,
                "run_id": run_id,
                "run_attempt": 1,
                "source_commit": "a" * 40,
                "archive_sha256": "b" * 64,
                "release_tag": "toronto-raw-snapshot-archive-2026-11",
                "asset_name": "toronto-raw-snapshot-bundle-123-1.zip",
                "order_timestamp": 1.0,
                "quarantined_row_count": 0,
                "quarantined_bad_row_count": 0,
                "quarantined_business_key_count": 0,
                "quarantine_sha256": None,
            }
            for source in ("active_permits", "cleared_permits")
        ],
    }
    path = tmp_path / "duckdb-summary.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def test_read_report_requires_full_rebuild_and_complete_two_source_ledger(tmp_path):
    path = _report(tmp_path)
    report, expected = promote._read_report(path, "123")
    assert report["history_scope"] == "full"
    assert len(expected) == 2

    value = json.loads(path.read_text())
    value["history_scope"] = "partial"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="full-history Mode 2"):
        promote._read_report(path, "123")


def test_read_report_rejects_archive_provenance_gap(tmp_path):
    path = _report(tmp_path)
    value = json.loads(path.read_text())
    value["ledger_entries"].pop()
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="two source ledger entries"):
        promote._read_report(path, "123")


def test_read_report_accepts_only_policy_bound_source_exclusion(tmp_path):
    path = _report(tmp_path)
    report = json.loads(path.read_text())
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
    path.write_text(json.dumps(report), encoding="utf-8")
    policy = {("123:1", "active_permits"): exclusion}

    with pytest.raises(ValueError, match="undeclared or mismatched exclusion"):
        promote._read_report(path, "123")

    _, expected = promote._read_report(
        path,
        "123",
        source_exclusions=policy,
    )
    assert expected["123:1|active_permits"]["warehouse_status"] == "excluded"
    assert expected["123:1|cleared_permits"]["warehouse_status"] == "succeeded"


def test_read_report_rejects_exclusion_checksum_mismatch(tmp_path):
    path = _report(tmp_path)
    report = json.loads(path.read_text())
    active = next(
        item
        for item in report["ledger_entries"]
        if item["source_name"] == "active_permits"
    )
    active["warehouse_status"] = "excluded"
    exclusion = {
        "snapshot_id": "123:1",
        "source_name": "active_permits",
        "archive_sha256": "c" * 64,
        "reason": "Test source undercount",
    }
    report["source_exclusions"] = [exclusion]
    path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(ValueError, match="checksum"):
        promote._read_report(
            path,
            "123",
            source_exclusions={("123:1", "active_permits"): exclusion},
        )


def test_promote_requires_human_confirmation_before_any_file_access(tmp_path):
    with pytest.raises(PermissionError, match="confirmation"):
        promote.promote(
            source_candidate=tmp_path / "source.duckdb",
            candidate=tmp_path / "candidate.duckdb",
            production=tmp_path / "production.duckdb",
            summary_path=tmp_path / "summary.json",
            run_id="123",
            confirmed=False,
        )


def test_promote_binds_candidate_to_run_evidence(tmp_path):
    summary = _report(tmp_path)
    policy_path = tmp_path / "source_replay_policy.json"
    policy_path.write_text(
        json.dumps({"policy_version": 1, "exclusions": []}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="source candidate path"):
        promote.promote(
            source_candidate=tmp_path / "wrong.duckdb",
            candidate=Path("data/warehouse/loading-candidate/toronto.duckdb"),
            production=Path("data/warehouse/toronto.duckdb"),
            summary_path=summary,
            run_id="123",
            confirmed=True,
            source_policy_path=policy_path,
        )
