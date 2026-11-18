from __future__ import annotations

import hashlib
import json
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from scripts.replay_archive import SOURCE_NAMES, extract_source, inspect_archive


def _write_archive(path, *, version):
    payload = {name: f"parquet payload for {name}".encode() for name in SOURCE_NAMES}
    sources = {}
    started = "2026-11-16T12:00:00+00:00"
    completed = "2026-11-16T12:00:05+00:00"
    for name, data in payload.items():
        digest = hashlib.sha256(data).hexdigest()
        if version == 1:
            manifest = {
                "source_name": name,
                "status": "success",
                "warehouse_status": "not_loaded",
                "warehouse_eligible": True,
                "row_count": 3,
                "columns": ["PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"],
                "sha256": digest,
                "extracted_at_utc": started,
            }
            summary = {
                "warehouse_eligible": True,
                "row_count": 3,
                "columns": manifest["columns"],
                "sha256": digest,
                "extracted_at_utc": started,
            }
        else:
            manifest = {
                "source_name": name,
                "validation_status": "passed",
                "resource_id": f"resource-{name}",
                "source_fingerprint": {"updated": "2026-11-16"},
                "extraction_method": "ckan_datastore",
                "row_count": 3,
                "columns": ["PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"],
                "required_columns": ["PERMIT_NUM"],
                "business_key": ["PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"],
                "parquet_size_bytes": len(data),
                "parquet_sha256": digest,
                "quality_warnings": [],
                "extraction_started_at_utc": started,
                "extraction_completed_at_utc": completed,
            }
            summary = {
                "data_file": f"{name}/data.parquet",
                "manifest_file": f"{name}/manifest.json",
                "row_count": 3,
                "columns": manifest["columns"],
                "parquet_size_bytes": len(data),
                "parquet_sha256": digest,
                "quality_warnings": [],
            }
        sources[name] = (manifest, summary, data)

    if version == 1:
        bundle = {
            "format_version": 1,
            "snapshot_stage": "raw",
            "source_run_id": "123456",
            "source_run_attempt": "2",
            "source_commit": "a" * 40,
            "extraction_started_at_utc": started,
            "complete": True,
            "approved_for_warehouse": True,
            "sources": {name: item[1] for name, item in sources.items()},
        }
        tag = "toronto-raw-snapshot-archive-2026-11"
        asset = "toronto-raw-snapshot-bundle-123456-2.zip"
    else:
        snapshot_id = "ed44fa3159ec485ba73fa70cfae938b8"
        bundle = {
            "format_version": 2,
            "snapshot_id": snapshot_id,
            "extraction_started_at_utc": started,
            "extraction_completed_at_utc": completed,
            "complete": True,
            "validation_status": "passed",
            "provenance": {
                "schema_version": 1,
                "producer": {
                    "package_name": "toronto-housing-extraction",
                    "package_version": "2.0.0",
                    "source_commit": "b" * 40,
                },
                "execution": {
                    "provider": "github_actions",
                    "repository": "gagadanalyst/toronto-housing-bottleneck-analytics",
                    "workflow": "V2 Snapshot Archive",
                    "run_id": "123457",
                    "run_attempt": 1,
                    "run_url": (
                        "https://github.com/gagadanalyst/"
                        "toronto-housing-bottleneck-analytics/actions/runs/123457"
                    ),
                },
            },
            "sources": {name: item[1] for name, item in sources.items()},
        }
        tag = "toronto-raw-snapshot-v2-archive-2026-11"
        asset = f"toronto-raw-snapshot-v2-bundle-{snapshot_id}.zip"

    with ZipFile(path, "w", ZIP_DEFLATED) as archive:
        archive.writestr("bundle.json", json.dumps(bundle))
        for name, (manifest, _, data) in sources.items():
            archive.writestr(f"{name}/manifest.json", json.dumps(manifest))
            archive.writestr(f"{name}/data.parquet", data)
    return tag, asset


@pytest.mark.parametrize("version", [1, 2])
def test_inspect_normalizes_both_archive_generations(tmp_path, version):
    path = tmp_path / "snapshot.zip"
    tag, asset = _write_archive(path, version=version)

    result = inspect_archive(
        path,
        tag,
        asset,
        repository="gagadanalyst/toronto-housing-bottleneck-analytics",
    )

    assert result.format_version == version
    assert set(result.sources) == set(SOURCE_NAMES)
    assert result.run_id in {"123456", "123457"}
    assert result.order[0].year == 2026


def test_extract_source_rechecks_archive_and_member_hashes(tmp_path):
    path = tmp_path / "snapshot.zip"
    tag, asset = _write_archive(path, version=2)
    snapshot = inspect_archive(
        path,
        tag,
        asset,
        repository="gagadanalyst/toronto-housing-bottleneck-analytics",
    )

    result = extract_source(snapshot, "active_permits", tmp_path / "staging")

    assert result.read_bytes() == b"parquet payload for active_permits"


def test_rejects_v2_archive_from_another_repository(tmp_path):
    path = tmp_path / "snapshot.zip"
    tag, asset = _write_archive(path, version=2)

    with pytest.raises(ValueError, match="different repository"):
        inspect_archive(path, tag, asset, repository="someone/else")


def test_parquet_checksum_failure_isolated_to_its_source(tmp_path):
    path = tmp_path / "snapshot.zip"
    tag, asset = _write_archive(path, version=2)
    with ZipFile(path) as source:
        entries = {name: source.read(name) for name in source.namelist()}
    entries["active_permits/data.parquet"] = b"tampered"
    with ZipFile(path, "w", ZIP_DEFLATED) as target:
        for name, payload in entries.items():
            target.writestr(name, payload)

    result = inspect_archive(
        path,
        tag,
        asset,
        repository="gagadanalyst/toronto-housing-bottleneck-analytics",
    )

    assert result.sources["active_permits"].error
    assert result.sources["cleared_permits"].error is None
    with pytest.raises(ValueError, match="archive validation failed"):
        extract_source(result, "active_permits", tmp_path / "active")
    assert (
        extract_source(result, "cleared_permits", tmp_path / "cleared").read_bytes()
        == b"parquet payload for cleared_permits"
    )


def test_missing_one_source_member_does_not_discard_the_other(tmp_path):
    path = tmp_path / "missing-active.zip"
    tag, asset = _write_archive(path, version=1)
    with ZipFile(path) as source:
        entries = {
            name: source.read(name)
            for name in source.namelist()
            if name != "active_permits/data.parquet"
        }
    with ZipFile(path, "w", ZIP_DEFLATED) as target:
        for name, payload in entries.items():
            target.writestr(name, payload)

    result = inspect_archive(path, tag, asset)

    assert result.sources["active_permits"].error
    assert result.sources["cleared_permits"].error is None


def test_v1_bundle_and_manifest_parquet_hashes_must_agree(tmp_path):
    path = tmp_path / "mismatched-v1.zip"
    tag, asset = _write_archive(path, version=1)
    with ZipFile(path) as source:
        entries = {name: source.read(name) for name in source.namelist()}
    bundle = json.loads(entries["bundle.json"])
    bundle["sources"]["active_permits"]["sha256"] = "0" * 64
    entries["bundle.json"] = json.dumps(bundle).encode()
    with ZipFile(path, "w", ZIP_DEFLATED) as target:
        for name, payload in entries.items():
            target.writestr(name, payload)

    result = inspect_archive(path, tag, asset)

    assert result.sources["active_permits"].error
    assert result.sources["cleared_permits"].error is None
