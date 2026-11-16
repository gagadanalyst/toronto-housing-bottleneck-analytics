import hashlib
import json
from pathlib import Path
from zipfile import ZIP_STORED, ZipFile

import pytest

from toronto_housing_extraction import release

REPOSITORY = "gagadanalyst/toronto-housing-bottleneck-analytics"
SOURCE_NAMES = ("active_permits", "cleared_permits")


def github_provenance():
    return {
        "schema_version": 1,
        "producer": {
            "package_name": "toronto-housing-extraction",
            "package_version": "0.1.0",
            "source_commit": "a" * 40,
        },
        "execution": {
            "provider": "github_actions",
            "repository": REPOSITORY,
            "workflow": "CD",
            "run_id": "123456",
            "run_attempt": 1,
            "run_url": (
                "https://github.com/"
                "gagadanalyst/toronto-housing-bottleneck-analytics/"
                "actions/runs/123456"
            ),
        },
    }


def make_archive(path: Path, *, snapshot_id="snapshot-001", data_suffix=b""):
    path.parent.mkdir(parents=True, exist_ok=True)
    sources = {}
    members = {}

    for source_name in SOURCE_NAMES:
        data_name = f"{source_name}/data.parquet"
        manifest_name = f"{source_name}/manifest.json"
        data = b"PAR1" + source_name.encode() + data_suffix
        digest = hashlib.sha256(data).hexdigest()
        columns = ["PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"]
        manifest = {
            "manifest_version": 1,
            "source_name": source_name,
            "resource_id": f"{source_name}-resource",
            "source_fingerprint": {"resource_id": f"{source_name}-resource"},
            "extraction_method": "datastore",
            "extraction_started_at_utc": "2026-11-16T05:00:00+00:00",
            "extraction_completed_at_utc": "2026-11-16T05:01:00+00:00",
            "row_count": 1,
            "columns": columns,
            "required_columns": columns,
            "business_key": ["PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"],
            "quality_warnings": [],
            "parquet_file": "data.parquet",
            "parquet_size_bytes": len(data),
            "parquet_sha256": digest,
            "validation_status": "passed",
        }
        sources[source_name] = {
            "data_file": data_name,
            "manifest_file": manifest_name,
            "row_count": 1,
            "columns": columns,
            "parquet_size_bytes": len(data),
            "parquet_sha256": digest,
            "quality_warnings": [],
        }
        members[data_name] = data
        members[manifest_name] = json.dumps(manifest, sort_keys=True).encode()

    bundle = {
        "format_version": 2,
        "snapshot_id": snapshot_id,
        "extraction_started_at_utc": "2026-11-16T05:00:00+00:00",
        "extraction_completed_at_utc": "2026-11-16T05:01:00+00:00",
        "complete": True,
        "validation_status": "passed",
        "provenance": github_provenance(),
        "sources": sources,
    }

    with ZipFile(path, "w", compression=ZIP_STORED) as archive:
        archive.writestr("bundle.json", json.dumps(bundle, sort_keys=True).encode())
        for member, content in members.items():
            archive.writestr(member, content)
    return path


def fake_github(
    monkeypatch, *, existing_assets=None, immutable=False, upload_error=False
):
    state = {
        "release": None,
        "assets": list(existing_assets or []),
        "asset_bytes": {},
        "upload_count": 0,
        "immutable": immutable,
        "upload_error": upload_error,
    }

    def run_gh(args, gh_executable):
        assert gh_executable == "gh"
        if (
            args[0] == "api"
            and args[1].startswith("repos/")
            and "/releases/tags/" in args[1]
        ):
            if state["release"] is None:
                raise release.GitHubCLIError("gh: Not Found (HTTP 404)")
            return json.dumps(state["release"])

        if args[0] == "api" and "--paginate" in args:
            return json.dumps([state["assets"]])

        if args[:2] == ["release", "create"]:
            tag = args[2]
            state["release"] = {
                "id": 77,
                "tag_name": tag,
                "draft": False,
                "prerelease": False,
                "immutable": state["immutable"],
            }
            return "created"

        if args[:2] == ["release", "upload"]:
            tag = args[2]
            path = Path(args[3])
            state["upload_count"] += 1
            content = path.read_bytes()
            state["asset_bytes"][path.name] = content
            state["assets"].append(
                {"id": 99, "name": path.name, "size": len(content), "state": "uploaded"}
            )
            if state["upload_error"]:
                raise release.GitHubCLIError("connection dropped after upload")
            return f"uploaded {tag}"

        if args[:2] == ["release", "download"]:
            name = args[args.index("--pattern") + 1]
            directory = Path(args[args.index("--dir") + 1])
            (directory / name).write_bytes(state["asset_bytes"][name])
            return "downloaded"

        raise AssertionError(f"Unexpected gh command: {args}")

    monkeypatch.setattr(release, "_run_gh", run_gh)
    return state


def test_inspects_valid_v2_archive_and_derives_release_identity(tmp_path):
    archive = make_archive(tmp_path / "snapshot.zip")

    result = release.inspect_snapshot_archive(archive, repository=REPOSITORY)

    assert result.snapshot_id == "snapshot-001"
    assert result.release_tag == "toronto-raw-snapshot-v2-archive-2026-11"
    assert result.asset_name == "toronto-raw-snapshot-v2-bundle-snapshot-001.zip"
    assert result.repository == REPOSITORY
    assert len(result.sha256) == 64


def test_rejects_local_provenance(tmp_path):
    archive = make_archive(tmp_path / "snapshot.zip")
    with ZipFile(archive) as source:
        members = {name: source.read(name) for name in source.namelist()}
    bundle = json.loads(members["bundle.json"])
    bundle["provenance"]["execution"]["provider"] = "local"
    members["bundle.json"] = json.dumps(bundle).encode()
    with ZipFile(archive, "w", compression=ZIP_STORED) as target:
        for name, content in members.items():
            target.writestr(name, content)

    with pytest.raises(ValueError, match="Only GitHub Actions"):
        release.inspect_snapshot_archive(archive, repository=REPOSITORY)


def test_rejects_parquet_checksum_mismatch(tmp_path):
    archive = make_archive(tmp_path / "snapshot.zip")
    with ZipFile(archive) as source:
        members = {name: source.read(name) for name in source.namelist()}
    changed = bytearray(members["active_permits/data.parquet"])
    changed[-1] ^= 1
    members["active_permits/data.parquet"] = bytes(changed)
    with ZipFile(archive, "w", compression=ZIP_STORED) as target:
        for name, content in members.items():
            target.writestr(name, content)

    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        release.inspect_snapshot_archive(archive, repository=REPOSITORY)


def test_rejects_mismatched_target_repository(tmp_path):
    archive = make_archive(tmp_path / "snapshot.zip")

    with pytest.raises(ValueError, match="does not match the target repository"):
        release.inspect_snapshot_archive(
            archive,
            repository="another-owner/another-repo",
        )


def test_publishes_and_download_verifies_new_asset(tmp_path, monkeypatch):
    archive = make_archive(tmp_path / "snapshot.zip")
    state = fake_github(monkeypatch)

    result = release.publish_snapshot_archive(archive, repository=REPOSITORY)

    assert result.already_present is False
    assert result.release_tag == "toronto-raw-snapshot-v2-archive-2026-11"
    assert state["upload_count"] == 1
    assert state["assets"][0]["name"] == result.asset_name
    assert state["asset_bytes"][result.asset_name] == archive.read_bytes()


def test_retry_of_identical_asset_is_idempotent(tmp_path, monkeypatch):
    archive = make_archive(tmp_path / "snapshot.zip")
    state = fake_github(monkeypatch)

    first = release.publish_snapshot_archive(archive, repository=REPOSITORY)
    second = release.publish_snapshot_archive(archive, repository=REPOSITORY)

    assert first.already_present is False
    assert second.already_present is True
    assert state["upload_count"] == 1


def test_rejects_different_bytes_for_existing_snapshot_id(tmp_path, monkeypatch):
    original = make_archive(tmp_path / "original.zip")
    conflicting = make_archive(
        tmp_path / "conflicting.zip",
        data_suffix=b"different source bytes",
    )
    state = fake_github(monkeypatch)

    release.publish_snapshot_archive(original, repository=REPOSITORY)
    with pytest.raises(RuntimeError, match="different metadata|different bytes"):
        release.publish_snapshot_archive(conflicting, repository=REPOSITORY)

    assert state["upload_count"] == 1


def test_recovers_when_upload_succeeded_but_client_lost_response(tmp_path, monkeypatch):
    archive = make_archive(tmp_path / "snapshot.zip")
    state = fake_github(monkeypatch, upload_error=True)

    result = release.publish_snapshot_archive(archive, repository=REPOSITORY)

    assert result.already_present is True
    assert state["upload_count"] == 1


def test_refuses_platform_immutable_monthly_release(tmp_path, monkeypatch):
    archive = make_archive(tmp_path / "snapshot.zip")
    fake_github(monkeypatch, immutable=True)

    with pytest.raises(RuntimeError, match="platform-immutable"):
        release.publish_snapshot_archive(archive, repository=REPOSITORY)


def test_verifies_existing_asset_on_platform_immutable_release(
    tmp_path,
    monkeypatch,
):
    archive = make_archive(tmp_path / "snapshot.zip")
    state = fake_github(monkeypatch)

    first = release.publish_snapshot_archive(
        archive,
        repository=REPOSITORY,
    )

    assert first.already_present is False

    # Simulate GitHub making the Release immutable after publication.
    state["immutable"] = True
    state["release"]["immutable"] = True

    second = release.publish_snapshot_archive(
        archive,
        repository=REPOSITORY,
    )

    assert second.already_present is True
    assert second.sha256 == first.sha256
    assert state["upload_count"] == 1
