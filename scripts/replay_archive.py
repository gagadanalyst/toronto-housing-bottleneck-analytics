"""Read and normalize approved V1/V2 Toronto snapshot archives.

This module intentionally knows about archive formats only. It does not discover
GitHub Releases, modify a warehouse, or maintain replay progress.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import BadZipFile, ZipFile

SOURCE_NAMES = ("active_permits", "cleared_permits")
EXPECTED_MEMBERS = {
    "bundle.json",
    *(
        f"{source}/{member}"
        for source in SOURCE_NAMES
        for member in ("data.parquet", "manifest.json")
    ),
}
V1_ASSET = re.compile(r"toronto-raw-snapshot-bundle-(\d+)-(\d+)\.zip\Z")
V2_ASSET = re.compile(
    r"toronto-raw-snapshot-v2-bundle-([A-Za-z0-9][A-Za-z0-9._-]{0,127})\.zip\Z"
)
SHA1 = re.compile(r"[0-9a-f]{40}\Z", re.IGNORECASE)
SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class ArchiveSource:
    """Validated source metadata and the archive members containing its data."""

    name: str
    data_member: str
    manifest_member: str
    manifest: dict[str, Any]
    row_count: int
    columns: tuple[str, ...]
    parquet_size_bytes: int
    parquet_sha256: str
    extracted_at: datetime
    error: str | None = None


@dataclass(frozen=True)
class SnapshotArchive:
    """Format-neutral view of one approved immutable archive."""

    path: Path
    format_version: int
    snapshot_id: str
    extracted_at: datetime
    source_commit: str
    run_id: str
    run_attempt: int
    release_tag: str
    asset_name: str
    archive_sha256: str
    sources: dict[str, ArchiveSource]
    bundle: dict[str, Any]

    @property
    def order(self) -> tuple[datetime, int, int, str]:
        """Chronological ordering shared by V1 and V2."""
        return (self.extracted_at, int(self.run_id), self.run_attempt, self.snapshot_id)


def _timestamp(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an ISO-8601 timestamp.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO-8601 timestamp.") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must include a timezone.")
    return parsed.astimezone(UTC)


def _json_member(archive: ZipFile, member: str) -> dict[str, Any]:
    try:
        value = json.loads(archive.read(member))
    except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Archive member {member!r} is not valid JSON.") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Archive member {member!r} must be a JSON object.")
    return value


def _sha256_member(archive: ZipFile, member: str) -> str:
    digest = hashlib.sha256()
    with archive.open(member) as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _archive_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_provenance(
    bundle: dict[str, Any], repository: str | None
) -> tuple[str, str, int]:
    provenance = bundle.get("provenance")
    if not isinstance(provenance, dict):
        raise ValueError("V2 bundle provenance is missing.")
    producer = provenance.get("producer")
    execution = provenance.get("execution")
    if not isinstance(producer, dict) or not isinstance(execution, dict):
        raise ValueError("V2 producer or execution provenance is missing.")
    if producer.get("package_name") != "toronto-housing-extraction":
        raise ValueError("V2 snapshot producer is not the Toronto extraction package.")
    if not str(producer.get("package_version", "")).strip():
        raise ValueError("V2 extraction package version is missing.")
    commit = producer.get("source_commit")
    if not isinstance(commit, str) or not (
        SHA1.fullmatch(commit) or SHA256.fullmatch(commit)
    ):
        raise ValueError("V2 source commit must be a full Git commit SHA.")
    if execution.get("provider") != "github_actions":
        raise ValueError("Only GitHub Actions V2 archives are replayable.")
    repo = execution.get("repository")
    if not isinstance(repo, str) or repo.count("/") != 1 or not all(repo.split("/")):
        raise ValueError("V2 repository provenance is invalid.")
    if repository is not None and repo.lower() != repository.lower():
        raise ValueError("V2 archive provenance belongs to a different repository.")
    run_id = execution.get("run_id")
    attempt = execution.get("run_attempt")
    if not isinstance(run_id, str) or not run_id.isdecimal() or int(run_id) < 1:
        raise ValueError("V2 GitHub run ID is invalid.")
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 1:
        raise ValueError("V2 GitHub run attempt is invalid.")
    workflow = execution.get("workflow")
    run_url = execution.get("run_url")
    if (
        not isinstance(workflow, str)
        or not workflow.strip()
        or not isinstance(run_url, str)
    ):
        raise ValueError("V2 workflow execution provenance is incomplete.")
    expected_suffix = f"/{repo}/actions/runs/{run_id}"
    if not run_url.startswith("https://") or not run_url.rstrip("/").endswith(
        expected_suffix
    ):
        raise ValueError("V2 run URL does not match its GitHub execution identity.")
    return commit, run_id, attempt


def _validate_v1(
    path: Path,
    release_tag: str,
    asset_name: str,
    bundle: dict[str, Any],
    archive: ZipFile,
) -> SnapshotArchive:
    match = V1_ASSET.fullmatch(asset_name)
    if not match:
        raise ValueError("V1 archive asset name is invalid.")
    asset_run, asset_attempt = match.groups()
    if (
        type(bundle.get("format_version")) is not int
        or bundle.get("format_version") != 1
    ):
        raise ValueError("Archive is not a supported V1 raw snapshot.")
    if bundle.get("snapshot_stage") != "raw":
        raise ValueError("Archive is not a supported V1 raw snapshot.")
    if (
        bundle.get("complete") is not True
        or bundle.get("approved_for_warehouse") is not True
    ):
        raise ValueError(
            "V1 archive is not complete and approved for warehouse loading."
        )
    run_id = str(bundle.get("source_run_id", ""))
    attempt = str(bundle.get("source_run_attempt", "1"))
    commit = str(bundle.get("source_commit", ""))
    if (run_id, attempt) != (asset_run, asset_attempt):
        raise ValueError("V1 archive asset identity does not match bundle metadata.")
    if int(run_id) < 1 or int(attempt) < 1:
        raise ValueError("V1 GitHub run ID and attempt must be positive integers.")
    if not SHA1.fullmatch(commit):
        raise ValueError("V1 source commit is invalid.")
    started = _timestamp(bundle.get("extraction_started_at_utc"), "V1 bundle start")
    if release_tag != f"toronto-raw-snapshot-archive-{started:%Y-%m}":
        raise ValueError("V1 Release month does not match extraction timestamp.")
    summaries = bundle.get("sources")
    if not isinstance(summaries, dict) or set(summaries) - set(SOURCE_NAMES):
        raise ValueError("V1 bundle has invalid source summaries.")

    sources: dict[str, ArchiveSource] = {}
    for name in SOURCE_NAMES:
        member = f"{name}/data.parquet"
        manifest_member = f"{name}/manifest.json"
        manifest: dict[str, Any] = {}
        try:
            summary = summaries.get(name)
            manifest = _json_member(archive, manifest_member)
            if not isinstance(summary, dict):
                raise ValueError("bundle source summary is missing or invalid")
            if (
                manifest.get("source_name") != name
                or manifest.get("status") != "success"
                or manifest.get("warehouse_status") != "not_loaded"
                or manifest.get("warehouse_eligible") is not True
                or summary.get("warehouse_eligible") is not True
            ):
                raise ValueError("source manifest is not eligible for warehouse replay")
            if any(
                summary.get(key)
                for key in ("validation_errors", "warehouse_quality_blockers")
            ) or any(
                manifest.get(key)
                for key in ("validation_errors", "warehouse_quality_blockers")
            ):
                raise ValueError(
                    "source archive contains validation or quality blockers"
                )
            columns = manifest.get("columns")
            row_count = manifest.get("row_count")
            digest = manifest.get("sha256")
            if (
                not isinstance(columns, list)
                or not columns
                or not all(isinstance(column, str) for column in columns)
            ):
                raise ValueError("source columns are invalid")
            if (
                not isinstance(row_count, int)
                or isinstance(row_count, bool)
                or row_count < 1
            ):
                raise ValueError("source row count is invalid")
            if row_count != summary.get("row_count") or columns != summary.get(
                "columns"
            ):
                raise ValueError("bundle and manifest row/schema metadata disagree")
            if (
                not isinstance(digest, str)
                or not SHA256.fullmatch(digest)
                or summary.get("sha256") != digest
            ):
                raise ValueError("source Parquet SHA-256 metadata is invalid")
            extracted_at = _timestamp(
                manifest.get("extracted_at_utc"), "V1 source extraction"
            )
            if extracted_at != _timestamp(
                summary.get("extracted_at_utc"), "V1 source summary extraction"
            ):
                raise ValueError("source extraction timestamps disagree")
            size = archive.getinfo(member).file_size
            if _sha256_member(archive, member) != digest:
                raise ValueError("source Parquet checksum mismatch")
            sources[name] = ArchiveSource(
                name,
                member,
                manifest_member,
                manifest,
                row_count,
                tuple(columns),
                size,
                digest,
                extracted_at,
            )
        except Exception as exc:
            sources[name] = ArchiveSource(
                name,
                member,
                manifest_member,
                manifest,
                0,
                (),
                0,
                "",
                started,
                f"{type(exc).__name__}: {exc}"[:4000],
            )

    return SnapshotArchive(
        path,
        1,
        f"{run_id}:{attempt}",
        started,
        commit,
        run_id,
        int(attempt),
        release_tag,
        asset_name,
        _archive_digest(path),
        sources,
        bundle,
    )


def _validate_v2(
    path: Path,
    release_tag: str,
    asset_name: str,
    bundle: dict[str, Any],
    archive: ZipFile,
    repository: str | None,
) -> SnapshotArchive:
    match = V2_ASSET.fullmatch(asset_name)
    if not match:
        raise ValueError("V2 archive asset name is invalid.")
    snapshot_id = match.group(1)
    if (
        type(bundle.get("format_version")) is not int
        or bundle.get("format_version") != 2
    ):
        raise ValueError("Archive is not a supported V2 snapshot.")
    if bundle.get("snapshot_id") != snapshot_id:
        raise ValueError("V2 archive asset name and bundle identity disagree.")
    if (
        bundle.get("complete") is not True
        or bundle.get("validation_status") != "passed"
    ):
        raise ValueError("V2 archive is incomplete or failed validation.")
    started = _timestamp(bundle.get("extraction_started_at_utc"), "V2 bundle start")
    completed = _timestamp(
        bundle.get("extraction_completed_at_utc"), "V2 bundle completion"
    )
    if completed < started:
        raise ValueError("V2 bundle completion precedes extraction start.")
    if release_tag != f"toronto-raw-snapshot-v2-archive-{started:%Y-%m}":
        raise ValueError("V2 Release month does not match extraction timestamp.")
    commit, run_id, attempt = _validate_provenance(bundle, repository)
    summaries = bundle.get("sources")
    if not isinstance(summaries, dict) or set(summaries) - set(SOURCE_NAMES):
        raise ValueError("V2 bundle has invalid source summaries.")

    sources: dict[str, ArchiveSource] = {}
    starts: list[datetime] = []
    completions: list[datetime] = []
    for name in SOURCE_NAMES:
        data_member = f"{name}/data.parquet"
        manifest_member = f"{name}/manifest.json"
        manifest: dict[str, Any] = {}
        try:
            summary = summaries.get(name)
            manifest = _json_member(archive, manifest_member)
            if not isinstance(summary, dict):
                raise ValueError("bundle source summary is missing or invalid")
            if (
                manifest.get("source_name") != name
                or manifest.get("validation_status") != "passed"
            ):
                raise ValueError("source manifest is not validated")
            if (
                not manifest.get("resource_id")
                or not isinstance(manifest.get("source_fingerprint"), dict)
                or not manifest.get("source_fingerprint")
                or not manifest.get("extraction_method")
            ):
                raise ValueError("source resource/fingerprint provenance is incomplete")
            columns = manifest.get("columns")
            required = manifest.get("required_columns")
            business_key = manifest.get("business_key")
            row_count = manifest.get("row_count")
            size = manifest.get("parquet_size_bytes")
            digest = manifest.get("parquet_sha256")
            if (
                not isinstance(columns, list)
                or not columns
                or not all(isinstance(column, str) and column for column in columns)
            ):
                raise ValueError("source columns are invalid")
            if (
                not isinstance(required, list)
                or not all(isinstance(column, str) for column in required)
                or not isinstance(business_key, list)
                or not business_key
                or not all(isinstance(column, str) for column in business_key)
            ):
                raise ValueError(
                    "source required-columns/business-key contract is invalid"
                )
            lower_columns = {column.lower() for column in columns}
            if not {column.lower() for column in required + business_key}.issubset(
                lower_columns
            ):
                raise ValueError("required or business-key columns are missing")
            if (
                not isinstance(row_count, int)
                or isinstance(row_count, bool)
                or row_count < 0
            ):
                raise ValueError("source row count is invalid")
            if (
                not isinstance(size, int)
                or isinstance(size, bool)
                or size < 1
                or not isinstance(digest, str)
                or not SHA256.fullmatch(digest)
            ):
                raise ValueError("source Parquet size or SHA-256 is invalid")
            if (
                summary.get("row_count") != row_count
                or summary.get("columns") != columns
            ):
                raise ValueError("bundle row/schema metadata mismatch")
            if (
                summary.get("parquet_size_bytes") != size
                or summary.get("parquet_sha256") != digest
            ):
                raise ValueError("bundle Parquet size/checksum metadata mismatch")
            if summary.get("quality_warnings") != manifest.get("quality_warnings"):
                raise ValueError("bundle and manifest quality warnings disagree")
            if (
                archive.getinfo(data_member).file_size != size
                or _sha256_member(archive, data_member) != digest
            ):
                raise ValueError("source Parquet size/checksum mismatch")
            source_start = _timestamp(
                manifest.get("extraction_started_at_utc"), "V2 source start"
            )
            source_end = _timestamp(
                manifest.get("extraction_completed_at_utc"), "V2 source completion"
            )
            if (
                source_start < started
                or source_end < source_start
                or source_end > completed
            ):
                raise ValueError("source extraction timestamps are inconsistent")
            starts.append(source_start)
            completions.append(source_end)
            sources[name] = ArchiveSource(
                name,
                data_member,
                manifest_member,
                manifest,
                row_count,
                tuple(columns),
                size,
                digest,
                source_start,
            )
        except Exception as exc:
            sources[name] = ArchiveSource(
                name,
                data_member,
                manifest_member,
                manifest,
                0,
                (),
                0,
                "",
                started,
                f"{type(exc).__name__}: {exc}"[:4000],
            )
    if len(starts) == len(SOURCE_NAMES) and (
        min(starts) != started or max(completions) != completed
    ):
        raise ValueError("V2 bundle and source extraction timestamps disagree.")
    return SnapshotArchive(
        path,
        2,
        snapshot_id,
        started,
        commit,
        run_id,
        attempt,
        release_tag,
        asset_name,
        _archive_digest(path),
        sources,
        bundle,
    )


def inspect_archive(
    path: Path,
    release_tag: str,
    asset_name: str,
    *,
    repository: str | None = None,
) -> SnapshotArchive:
    """Validate a V1 or V2 immutable archive and return normalized metadata.

    All archive members and both source manifests are checked before the caller
    is allowed to begin either source load.
    """
    path = Path(path)
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError("Snapshot archive is missing or empty.")
    try:
        with ZipFile(path) as archive:
            names = [item.filename for item in archive.infolist()]
            if len(names) != len(set(names)):
                raise ValueError("Snapshot archive contains duplicate member names.")
            if "bundle.json" not in names or set(names) - EXPECTED_MEMBERS:
                raise ValueError(
                    "Snapshot archive has missing bundle.json or unexpected members."
                )
            bundle = _json_member(archive, "bundle.json")
            if bundle.get("format_version") == 1:
                return _validate_v1(path, release_tag, asset_name, bundle, archive)
            if bundle.get("format_version") == 2:
                return _validate_v2(
                    path, release_tag, asset_name, bundle, archive, repository
                )
            raise ValueError(
                "Unsupported snapshot archive format: "
                f"{bundle.get('format_version')!r}."
            )
    except BadZipFile as exc:
        raise ValueError("Snapshot archive is not a valid ZIP file.") from exc


def extract_source(
    archive: SnapshotArchive, source_name: str, destination: Path
) -> Path:
    """Extract one already-validated Parquet member to a caller-owned directory."""
    if source_name not in archive.sources:
        raise ValueError(f"Unknown snapshot source: {source_name!r}.")
    source = archive.sources[source_name]
    if source.error:
        raise ValueError(f"{source_name} archive validation failed: {source.error}")
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / f"{source_name}.parquet"
    with ZipFile(archive.path) as zipped:
        # The member name comes from the fixed source allowlist, not ZIP paths.
        target.write_bytes(zipped.read(archive.sources[source_name].data_member))
    if _archive_digest(archive.path) != archive.archive_sha256:
        target.unlink(missing_ok=True)
        raise ValueError("Snapshot archive changed after validation.")
    if (
        target.stat().st_size != source.parquet_size_bytes
        or _file_sha256(target) != source.parquet_sha256
    ):
        target.unlink(missing_ok=True)
        raise ValueError(f"Extracted {source_name} Parquet failed its integrity check.")
    return target


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
