import hashlib
import sys

import pytest

import toronto_housing_extraction.preflight as preflight
from toronto_housing_extraction.preflight import source_fingerprints_match


def source(marker="2026-11-16T04:59:00.000000", *, reliable=True):
    return {
        "resource_id": "resource-1",
        "extraction_method": "datastore",
        "fingerprint_reliable": reliable,
        "source_fingerprint": {
            "fingerprint_version": 1,
            "resource_id": "resource-1",
            "datastore_cache_last_update": marker,
            "resource_last_modified": None,
            "package_metadata_modified": "metadata-only-value",
            "datastore_total": 196865,
            "total_was_estimated": False,
            "schema_sha256": "a" * 64,
        },
    }


def pair(active=None, cleared=None):
    return {
        "active_permits": active if active is not None else source(),
        "cleared_permits": cleared if cleared is not None else source(),
    }


def test_matches_only_when_both_source_fingerprints_are_reliable_and_equal():
    assert source_fingerprints_match(pair(), pair())


def test_ignores_package_metadata_modified_timestamp():
    current = pair()
    archived = pair()
    archived["active_permits"]["source_fingerprint"]["package_metadata_modified"] = (
        "different-package-timestamp"
    )

    assert source_fingerprints_match(current, archived)


def test_detects_change_in_either_source_marker():
    assert not source_fingerprints_match(
        pair(),
        pair(cleared=source("2026-11-16T05:00:00.000000")),
    )


def test_unreliable_source_never_allows_skip():
    assert not source_fingerprints_match(
        pair(active=source(reliable=False)),
        pair(),
    )


def test_missing_source_never_allows_skip():
    assert not source_fingerprints_match(
        {"active_permits": source()},
        pair(),
    )


def archive_asset():
    content = b"archive-content"
    return {
        "name": f"{preflight.V2_ASSET_PREFIX}snapshot-123.zip",
        "state": "uploaded",
        "digest": f"sha256:{hashlib.sha256(content).hexdigest()}",
        "size": len(content),
        "created_at": "2026-11-16T12:00:00Z",
    }


def sidecar_asset(content, *, size=None):
    return {
        "name": f"{preflight.FINGERPRINT_ASSET_PREFIX}snapshot-123.json",
        "state": "uploaded",
        "digest": f"sha256:{hashlib.sha256(content).hexdigest()}",
        "size": len(content) if size is None else size,
    }


def patch_latest_release(monkeypatch, assets):
    archive = archive_asset()
    release = {
        "tag_name": f"{preflight.V2_RELEASE_PREFIX}2026-11",
        "published_at": "2026-11-16T12:00:00Z",
        "draft": False,
        "prerelease": False,
        "assets": [archive, *assets],
    }
    monkeypatch.setattr(
        preflight,
        "_latest_bundle",
        lambda *_args: (release, archive),
    )


def test_missing_fingerprint_sidecar_requires_extraction(monkeypatch):
    patch_latest_release(monkeypatch, [])

    index, reason = preflight._load_latest_index("owner/repo", "gh")

    assert index is None
    assert "sidecar" in reason


def test_sidecar_checksum_mismatch_requires_extraction(monkeypatch):
    downloaded = b"tampered"
    declared = b"expected"

    patch_latest_release(
        monkeypatch,
        [sidecar_asset(declared, size=len(downloaded))],
    )
    monkeypatch.setattr(
        preflight,
        "_download_asset",
        lambda *_args: downloaded,
    )

    index, reason = preflight._load_latest_index("owner/repo", "gh")

    assert index is None
    assert "integrity" in reason


def test_sidecar_size_mismatch_requires_extraction(monkeypatch):
    downloaded = b"sidecar-content"

    patch_latest_release(
        monkeypatch,
        [sidecar_asset(downloaded, size=len(downloaded) + 1)],
    )
    monkeypatch.setattr(
        preflight,
        "_download_asset",
        lambda *_args: downloaded,
    )

    index, reason = preflight._load_latest_index("owner/repo", "gh")

    assert index is None
    assert "integrity" in reason


def test_github_api_failure_requires_extraction(monkeypatch, tmp_path, capsys):
    def fail_latest_bundle(*_args):
        raise RuntimeError("GitHub API unavailable")

    monkeypatch.setattr(preflight, "_latest_bundle", fail_latest_bundle)

    output = tmp_path / "github_output"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "toronto-check-snapshot",
            "--repository",
            "owner/repo",
            "--github-output",
            str(output),
        ],
    )

    assert preflight.main() == 0
    assert output.read_text(encoding="utf-8") == "no_change=false\n"
    assert "Extraction required" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("fingerprint_version", True),
        ("datastore_cache_last_update", "   "),
        ("total_was_estimated", "false"),
        ("schema_sha256", "not-a-valid-hash"),
    ],
)
def test_malformed_fingerprint_never_allows_skip(field, bad_value):
    current = pair()
    current["active_permits"]["source_fingerprint"][field] = bad_value

    assert not source_fingerprints_match(current, pair())


def test_missing_total_estimated_field_never_allows_skip():
    current = pair()
    del current["active_permits"]["source_fingerprint"]["total_was_estimated"]

    assert not source_fingerprints_match(current, pair())


@pytest.mark.parametrize("side", ["current", "archived"])
def test_resource_id_must_match_its_source_fingerprint(side):
    current = pair()
    archived = pair()

    target = current if side == "current" else archived
    target["active_permits"]["resource_id"] = "different-resource"

    assert not source_fingerprints_match(current, archived)


def test_resource_markers_are_stable_during_preflight():
    before = {
        "id": "resource-1",
        "datastore_cache_last_update": "2026-11-16T10:00:00",
        "last_modified": None,
    }

    assert preflight._resource_markers_unchanged(before, before.copy())


@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        ("id", "resource-2"),
        ("datastore_cache_last_update", "2026-11-16T10:05:00"),
        ("last_modified", "2026-11-16T10:05:00"),
    ],
)
def test_resource_marker_change_during_preflight_is_detected(field, changed_value):
    before = {
        "id": "resource-1",
        "datastore_cache_last_update": "2026-11-16T10:00:00",
        "last_modified": None,
    }
    after = {**before, field: changed_value}

    assert not preflight._resource_markers_unchanged(before, after)
