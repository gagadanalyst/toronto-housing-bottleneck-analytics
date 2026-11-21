from types import SimpleNamespace

import pytest

from scripts.source_replay_policy import (
    load_source_replay_policy,
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
