from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from scripts import replay_archive
from scripts import snapshot_inventory as inventory


def _archive(
    path: Path,
    *,
    version: int,
    day: int,
    run_id: str,
    snapshot_id: str,
    contract: bool = False,
):
    path.write_bytes(f"archive-{snapshot_id}".encode())
    extracted = datetime(2026, 11, day, tzinfo=UTC)
    asset = (
        f"toronto-raw-snapshot-bundle-{run_id}-1.zip"
        if version == 1
        else f"toronto-raw-snapshot-v2-bundle-{snapshot_id}.zip"
    )
    tag = (
        "toronto-raw-snapshot-archive-2026-11"
        if version == 1
        else "toronto-raw-snapshot-v2-archive-2026-11"
    )
    bundle = {}
    if contract:
        value = {
            "contract_id": inventory.CONTRACT_ID,
            "contract_version": 1,
            "normalization": {"version": 1},
        }
        bundle = {
            "replay_contract": value,
            "replay_contract_sha256": hashlib.sha256(
                json.dumps(
                    value,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ).encode()
            ).hexdigest(),
        }
    return SimpleNamespace(
        path=path,
        format_version=version,
        snapshot_id=snapshot_id,
        extracted_at=extracted,
        source_commit="a" * 40,
        run_id=run_id,
        run_attempt=1,
        release_tag=tag,
        asset_name=asset,
        archive_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        bundle=bundle,
        sources={
            "active_permits": SimpleNamespace(error=None),
            "cleared_permits": SimpleNamespace(error=None),
        },
        order=(extracted, int(run_id), 1, snapshot_id),
    )


def _asset(record):
    return {
        key: record[key]
        for key in (
            "release_tag",
            "asset_name",
            "snapshot_id",
            "archive_size_bytes",
            "archive_sha256",
        )
    }


def test_build_inventory_orders_records_and_legacy_contract_is_null(tmp_path):
    later = _archive(
        tmp_path / "later.zip", version=2, day=2, run_id="200", snapshot_id="later"
    )
    earlier = _archive(
        tmp_path / "earlier.zip", version=1, day=1, run_id="100", snapshot_id="100:1"
    )

    result = inventory.build_inventory("owner/repo", [later, earlier])

    assert [item["snapshot_id"] for item in result["archives"]] == ["100:1", "later"]
    assert result["archives"][0]["sequence"] == 1
    assert result["archives"][1]["replay_contract"] is None


def test_contract_reference_is_checked_and_recorded(tmp_path):
    archive = _archive(
        tmp_path / "contract.zip",
        version=2,
        day=2,
        run_id="200",
        snapshot_id="contracted",
        contract=True,
    )

    record = inventory.record_from_archive(archive, 1)

    assert record["replay_contract"] == {
        "contract_id": inventory.CONTRACT_ID,
        "contract_version": 1,
        "sha256": archive.bundle["replay_contract_sha256"],
    }
    archive.bundle["replay_contract"]["normalization"]["version"] = 9
    with pytest.raises(ValueError, match="checksum"):
        inventory.record_from_archive(archive, 1)


def test_partial_or_explicit_null_replay_contract_is_rejected(tmp_path):
    archive = _archive(
        tmp_path / "partial-contract.zip",
        version=2,
        day=2,
        run_id="200",
        snapshot_id="partial-contract",
    )
    archive.bundle["replay_contract"] = None
    with pytest.raises(ValueError, match="fields are incomplete"):
        inventory.record_from_archive(archive, 1)


def test_inventory_rejects_source_validation_errors(tmp_path):
    archive = _archive(
        tmp_path / "bad.zip", version=1, day=1, run_id="100", snapshot_id="100:1"
    )
    archive.sources["active_permits"].error = "invalid source manifest"
    with pytest.raises(ValueError, match="source validation failed"):
        inventory.record_from_archive(archive, 1)


def test_inventory_rejects_sequence_and_chronology_changes(tmp_path):
    first = _archive(
        tmp_path / "first.zip", version=1, day=1, run_id="100", snapshot_id="100:1"
    )
    second = _archive(
        tmp_path / "second.zip", version=1, day=2, run_id="200", snapshot_id="200:1"
    )
    result = inventory.build_inventory("owner/repo", [first, second])
    result["archives"][1]["sequence"] = 3
    with pytest.raises(ValueError, match="contiguous"):
        inventory.validate_inventory(result)

    result = inventory.build_inventory("owner/repo", [first, second])
    result["archives"].reverse()
    with pytest.raises(ValueError, match="contiguous|strict archive order"):
        inventory.validate_inventory(result)


def test_release_assets_must_match_inventory_exactly(tmp_path):
    archive = _archive(
        tmp_path / "one.zip", version=1, day=1, run_id="100", snapshot_id="100:1"
    )
    result = inventory.build_inventory("owner/repo", [archive])
    inventory.validate_inventory_assets(result, [_asset(result["archives"][0])])
    extra = dict(
        _asset(result["archives"][0]), asset_name="extra.zip", snapshot_id="extra"
    )
    with pytest.raises(ValueError, match="differ"):
        inventory.validate_inventory_assets(
            result, [_asset(result["archives"][0]), extra]
        )


def test_inspected_archives_must_match_inventory(tmp_path):
    archive = _archive(
        tmp_path / "one.zip", version=1, day=1, run_id="100", snapshot_id="100:1"
    )
    result = inventory.build_inventory("owner/repo", [archive])
    inventory.validate_inventory_archives(result, [archive], "owner/repo")
    archive.path.write_bytes(b"different bytes")
    with pytest.raises(ValueError, match="changed after inspection"):
        inventory.validate_inventory_archives(result, [archive], "owner/repo")


def test_append_is_idempotent_and_rejects_other_unindexed_assets(tmp_path, monkeypatch):
    baseline = _archive(
        tmp_path / "baseline.zip", version=1, day=1, run_id="100", snapshot_id="100:1"
    )
    new_archive = _archive(
        tmp_path / "toronto-raw-snapshot-v2-bundle-new-snapshot.zip",
        version=2,
        day=2,
        run_id="200",
        snapshot_id="new-snapshot",
        contract=True,
    )
    bundle = {
        "format_version": 2,
        "snapshot_id": "new-snapshot",
        "extraction_started_at_utc": "2026-11-02T00:00:00Z",
        **new_archive.bundle,
    }
    with ZipFile(new_archive.path, "w", ZIP_DEFLATED) as zipped:
        zipped.writestr("bundle.json", json.dumps(bundle))
    new_archive.archive_sha256 = hashlib.sha256(
        new_archive.path.read_bytes()
    ).hexdigest()
    # The appender obtains the fully inspected archive from the reader.
    monkeypatch.setattr(
        replay_archive, "inspect_archive", lambda *args, **kwargs: new_archive
    )

    baseline_inventory = inventory.build_inventory("owner/repo", [baseline])
    inventory_path = tmp_path / "snapshot_inventory.json"
    inventory.write_inventory(inventory_path, baseline_inventory, "owner/repo")
    pending = inventory.record_from_archive(new_archive, 2)
    monkeypatch.setattr(
        inventory,
        "list_release_assets",
        lambda _repo: [_asset(baseline_inventory["archives"][0]), _asset(pending)],
    )

    assert inventory.append_published_archive(
        new_archive.path, inventory_path, "owner/repo"
    )
    assert not inventory.append_published_archive(
        new_archive.path, inventory_path, "owner/repo"
    )
    assert len(inventory.load_inventory(inventory_path, "owner/repo")["archives"]) == 2

    def extra_assets(_repo):
        return [
            _asset(baseline_inventory["archives"][0]),
            _asset(pending),
            dict(_asset(pending), asset_name="unindexed.zip", snapshot_id="extra"),
        ]

    monkeypatch.setattr(inventory, "list_release_assets", extra_assets)
    with pytest.raises(ValueError, match="differ"):
        inventory.append_published_archive(
            new_archive.path, inventory_path, "owner/repo"
        )


def test_bootstrap_writes_only_the_verified_release_set(tmp_path, monkeypatch):
    archive = _archive(
        tmp_path / "baseline.zip",
        version=1,
        day=1,
        run_id="100",
        snapshot_id="100:1",
    )
    record = inventory.record_from_archive(archive, 1)
    monkeypatch.setattr(
        inventory, "list_release_assets", lambda _repo: [_asset(record)]
    )
    monkeypatch.setattr(
        inventory,
        "_download_inspected_archives",
        lambda _repo, _folder, _assets: [archive],
    )

    output = tmp_path / "snapshot_inventory.json"
    result = inventory.bootstrap_inventory(output, "owner/repo", expected_count=1)

    assert output.is_file()
    assert len(result["archives"]) == 1
    with pytest.raises(ValueError, match="refusing to overwrite"):
        inventory.bootstrap_inventory(output, "owner/repo", expected_count=1)


def test_release_asset_listing_excludes_quarantine_and_checks_sha256(monkeypatch):
    releases = [
        {"id": 1, "tag_name": "toronto-raw-snapshot-archive-2026-10"},
        {"id": 2, "tag_name": "toronto-raw-snapshot-v2-archive-2026-11"},
        {"id": 3, "tag_name": "toronto-raw-snapshot-quarantine-2026-11"},
    ]
    assets = {
        1: [
            {
                "name": "toronto-raw-snapshot-bundle-100-1.zip",
                "size": 10,
                "digest": "sha256:" + "a" * 64,
                "state": "uploaded",
            }
        ],
        2: [
            {
                "name": "toronto-raw-snapshot-v2-bundle-new-id.zip",
                "size": 20,
                "digest": "sha256:" + "b" * 64,
                "state": "uploaded",
            }
        ],
        3: [
            {
                "name": "toronto-raw-snapshot-bundle-999-1.zip",
                "size": 30,
                "digest": "sha256:" + "c" * 64,
                "state": "uploaded",
            }
        ],
    }

    def fake_gh_json(endpoint):
        if endpoint.endswith("releases?per_page=100"):
            return [releases]
        release_id = int(endpoint.split("/releases/")[1].split("/")[0])
        return [assets[release_id]]

    monkeypatch.setattr(inventory, "_gh_json", fake_gh_json)

    found = inventory.list_release_assets("owner/repo")

    assert [item["snapshot_id"] for item in found] == ["100:1", "new-id"]
    assert len(found) == 2


def test_release_asset_listing_rejects_missing_api_digest(monkeypatch):
    releases = [{"id": 1, "tag_name": "toronto-raw-snapshot-archive-2026-10"}]
    assets = [{"name": "toronto-raw-snapshot-bundle-100-1.zip", "size": 10}]

    def fake_gh_json(endpoint):
        return [releases] if endpoint.endswith("releases?per_page=100") else [assets]

    monkeypatch.setattr(inventory, "_gh_json", fake_gh_json)

    with pytest.raises(ValueError, match="SHA-256 digest"):
        inventory.list_release_assets("owner/repo")
