"""Validate and append V2 snapshot bundles to GitHub Releases."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import BadZipFile, ZipFile

from .snapshots import validate_release_provenance

SOURCE_NAMES = ("active_permits", "cleared_permits")
SNAPSHOT_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
V2_RELEASE_PREFIX = "toronto-raw-snapshot-v2-archive-"
V2_ASSET_PREFIX = "toronto-raw-snapshot-v2-bundle-"
FINGERPRINT_ASSET_PREFIX = "toronto-raw-snapshot-v2-fingerprint-"
MAX_RELEASE_ASSETS = 1000
MAX_ASSET_BYTES = 2 * 1024 * 1024 * 1024


class GitHubCLIError(RuntimeError):
    """A GitHub CLI command failed."""


@dataclass(frozen=True)
class SnapshotArchive:
    snapshot_id: str
    release_tag: str
    asset_name: str
    repository: str
    source_commit: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class PublicationResult:
    release_tag: str
    asset_name: str
    sha256: str
    size_bytes: int
    already_present: bool


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"Snapshot {field} timestamp is missing.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Snapshot {field} timestamp is invalid.") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"Snapshot {field} timestamp must include a timezone.")
    return parsed.astimezone(UTC)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(archive: ZipFile, member: str) -> dict[str, Any]:
    try:
        value = json.loads(archive.read(member))
    except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Snapshot member {member!r} is not valid JSON.") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Snapshot member {member!r} must contain a JSON object.")
    return value


def inspect_snapshot_archive(path: Path, repository: str) -> SnapshotArchive:
    """Check the V2 contract and source hashes before any network operation."""
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError("Snapshot archive is missing or empty.")
    size_bytes = path.stat().st_size
    if size_bytes >= MAX_ASSET_BYTES:
        raise ValueError("Snapshot archive exceeds GitHub's 2 GiB asset limit.")

    expected = {"bundle.json"}
    for source in SOURCE_NAMES:
        expected.update({f"{source}/data.parquet", f"{source}/manifest.json"})

    try:
        with ZipFile(path) as archive:
            names = [item.filename for item in archive.infolist()]
            if len(names) != len(expected) or set(names) != expected:
                raise ValueError(
                    "V2 archive must contain exactly the five expected files."
                )
            bad_member = archive.testzip()
            if bad_member:
                raise ValueError(f"ZIP integrity check failed for {bad_member!r}.")

            bundle = _read_json(archive, "bundle.json")
            if bundle.get("format_version") != 2:
                raise ValueError("Only V2 snapshot bundles can be published.")
            if (
                bundle.get("complete") is not True
                or bundle.get("validation_status") != "passed"
            ):
                raise ValueError(
                    "Only complete, validated snapshot bundles can be published."
                )

            snapshot_id = bundle.get("snapshot_id")
            if not isinstance(snapshot_id, str) or not SNAPSHOT_ID_PATTERN.fullmatch(
                snapshot_id
            ):
                raise ValueError("Snapshot ID is missing or unsafe for an asset name.")

            started = _timestamp(bundle.get("extraction_started_at_utc"), "start")
            completed = _timestamp(
                bundle.get("extraction_completed_at_utc"), "completion"
            )
            if completed < started:
                raise ValueError("Snapshot completion precedes extraction start.")

            sources = bundle.get("sources")
            if not isinstance(sources, dict) or set(sources) != set(SOURCE_NAMES):
                raise ValueError("V2 bundle must contain exactly Active and Cleared.")

            source_starts = []
            source_completions = []
            for source in SOURCE_NAMES:
                summary = sources[source]
                if not isinstance(summary, dict):
                    raise ValueError(f"{source}: bundle source summary is invalid.")

                data_member = f"{source}/data.parquet"
                manifest = _read_json(archive, f"{source}/manifest.json")
                if summary.get("data_file") != data_member:
                    raise ValueError(f"{source}: bundle Parquet path is invalid.")
                if summary.get("manifest_file") != f"{source}/manifest.json":
                    raise ValueError(f"{source}: bundle manifest path is invalid.")
                if (
                    manifest.get("source_name") != source
                    or manifest.get("validation_status") != "passed"
                ):
                    raise ValueError(
                        f"{source}: source manifest is invalid or unvalidated."
                    )
                if (
                    not manifest.get("resource_id")
                    or not isinstance(manifest.get("source_fingerprint"), dict)
                    or not manifest["source_fingerprint"]
                    or not manifest.get("extraction_method")
                ):
                    raise ValueError(
                        f"{source}: source resource/fingerprint metadata is missing."
                    )

                row_count = manifest.get("row_count")
                columns = manifest.get("columns")
                size = manifest.get("parquet_size_bytes")
                digest = manifest.get("parquet_sha256")
                if type(row_count) is not int or row_count < 0:
                    raise ValueError(f"{source}: source row count is invalid.")
                if (
                    not isinstance(columns, list)
                    or not columns
                    or not all(isinstance(column, str) and column for column in columns)
                ):
                    raise ValueError(
                        f"{source}: source columns are missing or invalid."
                    )
                required = manifest.get("required_columns")
                business_key = manifest.get("business_key")
                if not isinstance(required, list) or not isinstance(business_key, list):
                    raise ValueError(f"{source}: source field contract is missing.")
                if not all(
                    isinstance(column, str) for column in required + business_key
                ):
                    raise ValueError(f"{source}: source field contract is invalid.")
                actual_columns = {column.lower() for column in columns}
                if not {column.lower() for column in required}.issubset(actual_columns):
                    raise ValueError(f"{source}: required columns are missing.")
                if not business_key or not {
                    column.lower() for column in business_key
                }.issubset(actual_columns):
                    raise ValueError(f"{source}: business-key columns are missing.")
                if type(size) is not int or size < 1:
                    raise ValueError(f"{source}: Parquet size is invalid.")
                if not isinstance(digest, str) or not SHA256_PATTERN.fullmatch(digest):
                    raise ValueError(f"{source}: Parquet SHA-256 is invalid.")

                if (
                    summary.get("row_count") != row_count
                    or summary.get("columns") != columns
                ):
                    raise ValueError(f"{source}: bundle row/schema metadata mismatch.")
                if (
                    summary.get("parquet_size_bytes") != size
                    or summary.get("parquet_sha256") != digest
                ):
                    raise ValueError(f"{source}: bundle Parquet metadata mismatch.")
                if summary.get("quality_warnings") != manifest.get("quality_warnings"):
                    raise ValueError(f"{source}: bundle quality warnings mismatch.")
                if archive.getinfo(data_member).file_size != size:
                    raise ValueError(f"{source}: Parquet file size mismatch.")

                actual_digest = hashlib.sha256()
                with archive.open(data_member) as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                        actual_digest.update(chunk)
                if actual_digest.hexdigest() != digest:
                    raise ValueError(f"{source}: Parquet SHA-256 mismatch.")

                source_starts.append(
                    _timestamp(
                        manifest.get("extraction_started_at_utc"), f"{source} start"
                    )
                )
                source_completions.append(
                    _timestamp(
                        manifest.get("extraction_completed_at_utc"),
                        f"{source} completion",
                    )
                )
                if source_completions[-1] < source_starts[-1]:
                    raise ValueError(f"{source}: extraction completion precedes start.")

            if min(source_starts) != started or max(source_completions) != completed:
                raise ValueError(
                    "Bundle extraction timestamps do not match its manifests."
                )

            provenance = bundle.get("provenance")
            validate_release_provenance(provenance)
            if provenance["execution"].get("repository") != repository:
                raise ValueError(
                    "Snapshot provenance repository does not match the target repository."
                )
            source_commit = provenance["producer"]["source_commit"]

    except BadZipFile as exc:
        raise ValueError("Snapshot archive is not a valid ZIP file.") from exc

    return SnapshotArchive(
        snapshot_id=snapshot_id,
        release_tag=V2_RELEASE_PREFIX + started.strftime("%Y-%m"),
        asset_name=f"{V2_ASSET_PREFIX}{snapshot_id}.zip",
        repository=repository,
        source_commit=source_commit,
        sha256=_sha256_file(path),
        size_bytes=size_bytes,
    )


def _run_gh(args: list[str], executable: str) -> str:
    try:
        result = subprocess.run(
            [executable, *args], capture_output=True, text=True, check=False
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "GitHub CLI 'gh' was not found or is not installed."
        ) from exc
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
        raise GitHubCLIError(f"gh {' '.join(args)} failed: {detail}")
    return result.stdout


def _release(repository: str, tag: str, gh: str) -> dict[str, Any] | None:
    try:
        output = _run_gh(["api", f"repos/{repository}/releases/tags/{tag}"], gh)
    except GitHubCLIError as exc:
        if "HTTP 404" in str(exc) or "Not Found" in str(exc):
            return None
        raise
    value = json.loads(output)
    if not isinstance(value, dict):
        raise RuntimeError("GitHub returned an invalid Release object.")
    return value


def _assets(repository: str, release_id: int, gh: str) -> list[dict[str, Any]]:
    endpoint = f"repos/{repository}/releases/{release_id}/assets?per_page=100"
    pages = json.loads(_run_gh(["api", "--paginate", "--slurp", endpoint], gh) or "[]")
    if not isinstance(pages, list):
        raise RuntimeError("GitHub returned an invalid Release asset list.")
    assets = []
    for page in pages:
        if isinstance(page, list):
            assets.extend(item for item in page if isinstance(item, dict))
        elif isinstance(page, dict):
            assets.append(page)
        else:
            raise RuntimeError("GitHub returned an invalid Release asset page.")
    return assets


def _find_asset(assets: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    matches = [item for item in assets if item.get("name") == name]
    if len(matches) > 1:
        raise RuntimeError(f"Release contains duplicate assets named {name!r}.")
    return matches[0] if matches else None


def _download_hash(repository: str, tag: str, name: str, gh: str) -> str:
    with tempfile.TemporaryDirectory(prefix="toronto-v2-verify-") as directory:
        _run_gh(
            [
                "release",
                "download",
                tag,
                "--repo",
                repository,
                "--pattern",
                name,
                "--dir",
                directory,
            ],
            gh,
        )
        downloaded = Path(directory) / name
        if not downloaded.is_file() or not downloaded.stat().st_size:
            raise RuntimeError("GitHub did not download the expected Release asset.")
        return _sha256_file(downloaded)


def _verify_asset(asset: dict[str, Any], snapshot: SnapshotArchive, gh: str) -> None:
    if (
        asset.get("state") not in (None, "uploaded")
        or asset.get("size") != snapshot.size_bytes
    ):
        raise RuntimeError(
            "Existing Release asset has different metadata; refusing overwrite."
        )
    if (
        _download_hash(
            snapshot.repository, snapshot.release_tag, snapshot.asset_name, gh
        )
        != snapshot.sha256
    ):
        raise RuntimeError(
            "Existing Release asset has different bytes; refusing overwrite."
        )


def _ensure_release(snapshot: SnapshotArchive, gh: str) -> dict[str, Any]:
    release = _release(snapshot.repository, snapshot.release_tag, gh)
    if release is None:
        month = snapshot.release_tag.removeprefix(V2_RELEASE_PREFIX)
        notes = (
            "Append-only V2 archives of complete, validated Active and Cleared "
            "Permits source snapshots. Assets are verified by re-downloading and "
            "comparing SHA-256. No asset replacement or automatic pruning is "
            "performed. V1 archives remain in their existing Release namespace."
        )
        try:
            _run_gh(
                [
                    "release",
                    "create",
                    snapshot.release_tag,
                    "--repo",
                    snapshot.repository,
                    "--title",
                    f"Toronto raw snapshot V2 archive {month}",
                    "--notes",
                    notes,
                    "--target",
                    snapshot.source_commit,
                ],
                gh,
            )
        except GitHubCLIError:
            # A retry may have created the Release concurrently. Re-read it.
            release = _release(snapshot.repository, snapshot.release_tag, gh)
            if release is None:
                raise
        else:
            release = _release(snapshot.repository, snapshot.release_tag, gh)

    if release is None or release.get("tag_name") != snapshot.release_tag:
        raise RuntimeError("Expected V2 monthly GitHub Release was not found.")
    if release.get("draft") is not False or release.get("prerelease") is not False:
        raise RuntimeError(
            "V2 archive Release must be published, not draft or prerelease."
        )
    return release


def _fingerprint_index(archive_path: Path, snapshot: SnapshotArchive) -> dict[str, Any]:
    """Build the sidecar index from the already-validated V2 archive."""
    with ZipFile(archive_path) as archive:
        bundle = _read_json(archive, "bundle.json")
        sources: dict[str, Any] = {}
        for source_name in SOURCE_NAMES:
            manifest = _read_json(archive, f"{source_name}/manifest.json")
            sources[source_name] = {
                "resource_id": manifest.get("resource_id"),
                "extraction_method": manifest.get("extraction_method"),
                "source_fingerprint": manifest.get("source_fingerprint"),
                # Older bundles lack this field and are intentionally unreliable.
                "fingerprint_reliable": manifest.get("fingerprint_reliable") is True,
            }

    return {
        "index_version": 1,
        "snapshot_id": snapshot.snapshot_id,
        "release_tag": snapshot.release_tag,
        "archive_asset_name": snapshot.asset_name,
        "archive_sha256": snapshot.sha256,
        "archive_size_bytes": snapshot.size_bytes,
        "extraction_completed_at_utc": bundle.get("extraction_completed_at_utc"),
        "sources": sources,
    }


def _verify_exact_asset(
    asset: dict[str, Any],
    *,
    repository: str,
    tag: str,
    name: str,
    sha256_digest: str,
    size_bytes: int,
    gh: str,
) -> None:
    """Verify a Release asset by metadata and a fresh download."""
    if asset.get("state") not in (None, "uploaded") or asset.get("size") != size_bytes:
        raise RuntimeError(
            f"Existing Release asset {name!r} has different metadata; "
            "refusing overwrite."
        )
    if _download_hash(repository, tag, name, gh) != sha256_digest:
        raise RuntimeError(
            f"Existing Release asset {name!r} has different bytes; refusing overwrite."
        )


def _ensure_fingerprint_asset(
    snapshot: SnapshotArchive,
    archive_path: Path,
    release: dict[str, Any],
    release_id: int,
    gh: str,
) -> None:
    """Append and verify the fingerprint sidecar after the ZIP is verified."""
    index = _fingerprint_index(archive_path, snapshot)
    asset_name = f"{FINGERPRINT_ASSET_PREFIX}{snapshot.snapshot_id}.json"
    content = (json.dumps(index, indent=2, sort_keys=True) + "\n").encode("utf-8")
    digest = hashlib.sha256(content).hexdigest()
    size_bytes = len(content)

    if size_bytes >= MAX_ASSET_BYTES:
        raise ValueError("Fingerprint sidecar exceeds GitHub's 2 GiB asset limit.")

    assets = _assets(snapshot.repository, release_id, gh)
    existing = _find_asset(assets, asset_name)
    if existing:
        _verify_exact_asset(
            existing,
            repository=snapshot.repository,
            tag=snapshot.release_tag,
            name=asset_name,
            sha256_digest=digest,
            size_bytes=size_bytes,
            gh=gh,
        )
        return

    if release.get("immutable") is True:
        raise RuntimeError(
            "Monthly Release is platform-immutable and cannot accept a "
            "missing fingerprint sidecar."
        )
    if len(assets) >= MAX_RELEASE_ASSETS:
        raise RuntimeError(
            "Release reached GitHub's 1,000-asset limit; no assets were removed."
        )

    try:
        with tempfile.TemporaryDirectory(prefix="toronto-v2-index-") as directory:
            index_path = Path(directory) / asset_name
            index_path.write_bytes(content)
            _run_gh(
                [
                    "release",
                    "upload",
                    snapshot.release_tag,
                    str(index_path),
                    "--repo",
                    snapshot.repository,
                ],
                gh,
            )
    except GitHubCLIError:
        # The upload may have succeeded even if the client lost the response.
        assets = _assets(snapshot.repository, release_id, gh)
        uploaded = _find_asset(assets, asset_name)
        if uploaded is None:
            raise
        _verify_exact_asset(
            uploaded,
            repository=snapshot.repository,
            tag=snapshot.release_tag,
            name=asset_name,
            sha256_digest=digest,
            size_bytes=size_bytes,
            gh=gh,
        )
    else:
        assets = _assets(snapshot.repository, release_id, gh)
        uploaded = _find_asset(assets, asset_name)
        if uploaded is None:
            raise RuntimeError(
                "Fingerprint sidecar upload completed but the asset is not listed."
            )
        _verify_exact_asset(
            uploaded,
            repository=snapshot.repository,
            tag=snapshot.release_tag,
            name=asset_name,
            sha256_digest=digest,
            size_bytes=size_bytes,
            gh=gh,
        )


def publish_snapshot_archive(
    archive_path: Path,
    *,
    repository: str | None = None,
    gh_executable: str = "gh",
) -> PublicationResult:
    """Publish one validated V2 ZIP as an append-only GitHub Release asset."""
    repo = (repository or os.environ.get("GITHUB_REPOSITORY", "")).strip()
    if not repo or repo.count("/") != 1 or not all(repo.split("/")):
        raise ValueError("Target repository is required as GitHub owner/name.")
    snapshot = inspect_snapshot_archive(archive_path, repo)
    release = _ensure_release(snapshot, gh_executable)
    release_id = release.get("id")
    if type(release_id) is not int or release_id < 1:
        raise RuntimeError("GitHub Release ID is missing or invalid.")

    assets = _assets(repo, release_id, gh_executable)
    existing = _find_asset(assets, snapshot.asset_name)
    if existing:
        _verify_asset(existing, snapshot, gh_executable)
        already_present = True
    else:
        if release.get("immutable") is True:
            raise RuntimeError(
                "Monthly Release is platform-immutable and cannot accept new assets."
            )
        if len(assets) >= MAX_RELEASE_ASSETS:
            raise RuntimeError(
                "Release reached GitHub's 1,000-asset limit; no assets were removed."
            )
        try:
            # GitHub uses the local basename as the asset name; use the canonical V2 name.
            with tempfile.TemporaryDirectory(prefix="toronto-v2-upload-") as directory:
                upload_path = Path(directory) / snapshot.asset_name
                shutil.copyfile(archive_path, upload_path)
                _run_gh(
                    [
                        "release",
                        "upload",
                        snapshot.release_tag,
                        str(upload_path),
                        "--repo",
                        repo,
                    ],
                    gh_executable,
                )
        except GitHubCLIError:
            # The server may have accepted the upload before the client lost its response.
            assets = _assets(repo, release_id, gh_executable)
            existing = _find_asset(assets, snapshot.asset_name)
            if existing is None:
                raise
            _verify_asset(existing, snapshot, gh_executable)
            already_present = True
        else:
            assets = _assets(repo, release_id, gh_executable)
            uploaded = _find_asset(assets, snapshot.asset_name)
            if uploaded is None:
                raise RuntimeError(
                    "Upload completed but the asset is not listed in the Release."
                )
            _verify_asset(uploaded, snapshot, gh_executable)
            already_present = False

    # Publish the small index only after the large archive has been verified.
    _ensure_fingerprint_asset(
        snapshot,
        archive_path,
        release,
        release_id,
        gh_executable,
    )

    return PublicationResult(
        release_tag=snapshot.release_tag,
        asset_name=snapshot.asset_name,
        sha256=snapshot.sha256,
        size_bytes=snapshot.size_bytes,
        already_present=already_present,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--gh", default="gh", help="GitHub CLI executable.")
    args = parser.parse_args()
    try:
        result = publish_snapshot_archive(
            args.archive, repository=args.repository, gh_executable=args.gh
        )
    except (
        GitHubCLIError,
        RuntimeError,
        ValueError,
        OSError,
        json.JSONDecodeError,
    ) as exc:
        print(f"Snapshot archive publication failed: {exc}", file=sys.stderr)
        return 1

    status = (
        "Already present and verified"
        if result.already_present
        else "Published and verified"
    )
    print(f"{status}: {result.asset_name}")
    print(f"Release: {result.release_tag}")
    print(f"SHA-256: {result.sha256}")
    print(f"Size: {result.size_bytes:,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
