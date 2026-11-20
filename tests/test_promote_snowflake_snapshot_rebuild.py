from __future__ import annotations

import hashlib
import json

import pytest

from scripts import promote_snowflake_snapshot_rebuild as promote


@pytest.fixture(autouse=True)
def _enable_snowflake_operations(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_OPERATIONS_ENABLED", "true")


def _summary(tmp_path):
    value = {
        "backend": "snowflake",
        "status": "passed",
        "mode": "rebuild",
        "history_scope": "full",
        "full_history": True,
        "explicit_promotion_required": True,
        "warehouse_input_staging": "not_used",
        "candidate": "TORONTO.TORONTO_SNAPSHOT_REPLAY_123",
        "snapshot_count_discovered": 1,
        "snapshot_count_selected": 1,
        "selection": {"from_date": None, "to_date": None, "limit": None},
        "snapshots": [{"snapshot_id": "123:1"}],
        "ledger_entries": [
            {
                "snapshot_id": "123:1",
                "source_name": source,
                "status": "succeeded",
                "run_id": "123",
                "run_attempt": 1,
                "source_commit": "a" * 40,
                "release_tag": "toronto-raw-snapshot-archive-2026-11",
                "asset_name": "toronto-raw-snapshot-bundle-123-1.zip",
                "format_version": 1,
                "archive_sha256": "b" * 64,
                "order_timestamp": 1.0,
                "quarantined_row_count": 0,
                "quarantined_bad_row_count": 0,
                "quarantined_business_key_count": 0,
                "quarantine_sha256": None,
            }
            for source in ("active_permits", "cleared_permits")
        ],
    }
    path = tmp_path / "snowflake-summary.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def test_expected_snowflake_ledger_requires_full_rebuild_evidence(tmp_path):
    path = _summary(tmp_path)
    values = promote._expected_ledger_from_summary(path, "123")
    assert set(values) == {"123:1|active_permits", "123:1|cleared_permits"}
    value = json.loads(path.read_text())
    value["history_scope"] = "partial"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="full-history candidate"):
        promote._expected_ledger_from_summary(path, "123")


def test_quarantine_reconciliation_accepts_verified_empty_history():
    empty = hashlib.sha256(b"").hexdigest()
    ledger = [
        (
            "123:1",
            "123",
            1,
            source,
            "succeeded",
            "commit",
            "tag",
            "asset",
            1,
            "a" * 64,
            0,
            0,
            0,
            empty,
        )
        for source in ("active_permits", "cleared_permits")
    ]
    promote._assert_quarantine_reconciliation(
        object(), ledger, [], "TORONTO_SNAPSHOT_REPLAY_123"
    )


def test_quarantine_reconciliation_rejects_ledger_count_mismatch():
    empty = hashlib.sha256(b"").hexdigest()
    ledger = [
        (
            "123:1",
            "123",
            1,
            "active_permits",
            "succeeded",
            "commit",
            "tag",
            "asset",
            1,
            "a" * 64,
            1,
            1,
            1,
            empty,
        )
    ]
    with pytest.raises(RuntimeError, match="quarantine reconciliation failed"):
        promote._assert_quarantine_reconciliation(
            object(), ledger, [], "TORONTO_SNAPSHOT_REPLAY_123"
        )


def test_snowflake_promoter_requires_run_scoped_candidate_and_confirmation(monkeypatch):
    monkeypatch.setattr(
        promote,
        "_connection",
        lambda _uri: (_ for _ in ()).throw(AssertionError("must not connect")),
    )
    with pytest.raises(ValueError, match="Unsafe Mode 2"):
        promote.promote("unused", "RAW", "123", 1, confirmed=True, summary_path=None)
    with pytest.raises(ValueError, match="does not belong"):
        promote.promote(
            "unused",
            "TORONTO_SNAPSHOT_REPLAY_999",
            "123",
            1,
            confirmed=True,
            summary_path=None,
        )
    with pytest.raises(PermissionError, match="confirmation"):
        promote.promote(
            "unused",
            "TORONTO_SNAPSHOT_REPLAY_123",
            "123",
            1,
            confirmed=False,
            summary_path=None,
        )


def test_snowflake_snapshot_promoter_rejects_disabled_gate(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_OPERATIONS_ENABLED", "false")

    with pytest.raises(PermissionError, match="SNOWFLAKE_OPERATIONS_ENABLED"):
        promote.promote(
            "unused",
            "TORONTO_SNAPSHOT_REPLAY_123",
            "123",
            1,
            confirmed=True,
            summary_path=None,
        )
