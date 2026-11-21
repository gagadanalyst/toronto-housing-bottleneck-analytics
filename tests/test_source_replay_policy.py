from types import SimpleNamespace

import pytest

from scripts.source_replay_policy import (
    load_source_replay_policy,
    validate_report_source_exclusions,
    validate_source_replay_policy,
)


def _record():
    return {
        "snapshot_id": "snapshot-a",
        "source_name": "active_permits",
        "archive_sha256": "a" * 64,
        "reason": "Transient CKAN source undercount",
    }


def test_loads_valid_source_replay_policy(tmp_path):
    path = tmp_path / "source_replay_policy.json"
    path.write_text(
        """
        {
          "policy_version": 1,
          "exclusions": [
            {
              "snapshot_id": "snapshot-a",
              "source_name": "active_permits",
              "archive_sha256": "%s",
              "reason": "Transient CKAN source undercount"
            }
          ]
        }
        """
        % ("a" * 64),
        encoding="utf-8",
    )

    result = load_source_replay_policy(path)

    assert result == {("snapshot-a", "active_permits"): _record()}


def test_rejects_duplicate_source_exclusions(tmp_path):
    record = _record()
    path = tmp_path / "source_replay_policy.json"
    import json

    path.write_text(
        json.dumps(
            {
                "policy_version": 1,
                "exclusions": [record, record],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate source exclusion"):
        load_source_replay_policy(path)


def test_rejects_archive_checksum_mismatch():
    archive = SimpleNamespace(
        snapshot_id="snapshot-a",
        archive_sha256="b" * 64,
    )
    exclusions = {("snapshot-a", "active_permits"): _record()}

    with pytest.raises(ValueError, match="checksum mismatch"):
        validate_source_replay_policy([archive], exclusions)


def test_full_inventory_requires_every_policy_exclusion():
    exclusions = {("snapshot-a", "active_permits"): _record()}

    with pytest.raises(ValueError, match="missing policy exclusions"):
        validate_source_replay_policy([], exclusions, require_all=True)


def test_summary_exclusion_requires_matching_policy_and_warehouse_status():
    exclusion = _record()
    report = {
        "history_scope": "full",
        "snapshots": [{"snapshot_id": "snapshot-a"}],
        "source_exclusions": [exclusion],
        "ledger_entries": [
            {
                "snapshot_id": "snapshot-a",
                "source_name": "active_permits",
                "warehouse_status": "excluded",
                "archive_sha256": "a" * 64,
            },
            {
                "snapshot_id": "snapshot-a",
                "source_name": "cleared_permits",
                "warehouse_status": "succeeded",
                "archive_sha256": "a" * 64,
            },
        ],
    }
    policy = {("snapshot-a", "active_permits"): exclusion}

    statuses = validate_report_source_exclusions(report, policy)

    assert statuses == {
        ("snapshot-a", "active_permits"): "excluded",
        ("snapshot-a", "cleared_permits"): "succeeded",
    }


def test_summary_rejects_excluded_source_without_policy():
    exclusion = _record()
    report = {
        "history_scope": "full",
        "snapshots": [{"snapshot_id": "snapshot-a"}],
        "source_exclusions": [exclusion],
        "ledger_entries": [
            {
                "snapshot_id": "snapshot-a",
                "source_name": "active_permits",
                "warehouse_status": "excluded",
                "archive_sha256": "a" * 64,
            },
            {
                "snapshot_id": "snapshot-a",
                "source_name": "cleared_permits",
                "warehouse_status": "succeeded",
                "archive_sha256": "a" * 64,
            },
        ],
    }

    with pytest.raises(ValueError, match="undeclared or mismatched exclusion"):
        validate_report_source_exclusions(report, {})


def test_summary_rejects_policy_exclusion_that_was_loaded():
    exclusion = _record()
    report = {
        "history_scope": "full",
        "snapshots": [{"snapshot_id": "snapshot-a"}],
        "source_exclusions": [exclusion],
        "ledger_entries": [
            {
                "snapshot_id": "snapshot-a",
                "source_name": "active_permits",
                "warehouse_status": "succeeded",
                "archive_sha256": "a" * 64,
            },
            {
                "snapshot_id": "snapshot-a",
                "source_name": "cleared_permits",
                "warehouse_status": "succeeded",
                "archive_sha256": "a" * 64,
            },
        ],
    }

    with pytest.raises(ValueError, match="warehouse status"):
        validate_report_source_exclusions(
            report,
            {("snapshot-a", "active_permits"): exclusion},
        )
