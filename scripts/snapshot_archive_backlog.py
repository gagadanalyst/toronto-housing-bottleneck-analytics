"""Discover and order immutable Toronto raw snapshot Release assets."""

from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import BadZipFile, ZipFile

MONTHLY_TAG_RE = re.compile(r"toronto-raw-snapshot-archive-(\d{4})-(\d{2})\Z")
ASSET_RE = re.compile(r"toronto-raw-snapshot-bundle-(\d+)-(\d+)\.zip\Z")
SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
SOURCE_NAMES = ("active_permits", "cleared_permits")


@dataclass(frozen=True)
class SnapshotArchive:
    """One immutable raw archive and its validated ordering metadata."""

    release_tag: str
    asset_name: str
    path: Path
    bundle: dict[str, Any]
    extracted_at: datetime
    run_id: str
    attempt: str

    @property
    def snapshot_id(self) -> str:
        return f"{self.run_id}:{self.attempt}"

    @property
    def order(self) -> tuple[datetime, int, int]:
        return snapshot_order(self.extracted_at, self.run_id, self.attempt)


def _utc_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"Invalid ISO-8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError("Snapshot timestamps must include a timezone.")
    return parsed.astimezone(UTC)


def snapshot_order(
    extracted_at: str | datetime, run_id: str, attempt: str | int
) -> tuple[datetime, int, int]:
    """Return a deterministic chronological key for an archived snapshot."""
    if not str(run_id).isdigit() or not str(attempt).isdigit():
        raise ValueError("Snapshot run ID and attempt must contain digits only.")
    return (_utc_datetime(extracted_at), int(run_id), int(attempt))


def _run_gh_json(*args: str) -> Any:
    result = subprocess.run(
        ["gh", "api", "--paginate", "--slurp", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        return json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"GitHub API returned invalid JSON for {args[-1]!r}."
        ) from exc


def _flatten_pages(value: Any) -> list[dict[str, Any]]:
    """Flatten gh --paginate --slurp output across CLI versions."""
    flattened: list[dict[str, Any]] = []
    if isinstance(value, list):
        for item in value:
            flattened.extend(_flatten_pages(item))
    elif isinstance(value, dict):
        flattened.append(value)
    return flattened


def _monthly_release_assets(repository: str) -> list[tuple[str, str, int]]:
    releases = _flatten_pages(_run_gh_json(f"repos/{repository}/releases?per_page=100"))
    matches: list[tuple[str, str, int]] = []
    for release in releases:
        tag = str(release.get("tag_name", ""))
        if (
            not MONTHLY_TAG_RE.fullmatch(tag)
            or release.get("draft")
            or release.get("prerelease")
        ):
            continue
        release_id = release.get("id")
        if not release_id:
            raise RuntimeError(f"Monthly Release {tag} has no numeric Release ID.")
        assets = _flatten_pages(
            _run_gh_json(
                f"repos/{repository}/releases/{int(release_id)}/assets?per_page=100"
            )
        )
        for asset in assets:
            name = str(asset.get("name", ""))
            if ASSET_RE.fullmatch(name):
                size = int(asset.get("size", 0))
                if size <= 0 or size >= 2_147_483_648:
                    raise RuntimeError(
                        f"Archive asset {tag}/{name} has invalid or unsupported size: {size}."
                    )
                matches.append((tag, name, int(release_id)))
    return matches


def _download_asset(
    repository: str, tag: str, asset_name: str, destination: Path
) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "gh",
            "release",
            "download",
            tag,
            "--repo",
            repository,
            "--pattern",
            asset_name,
            "--dir",
            str(destination),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    path = destination / asset_name
    if not path.is_file() or path.stat().st_size == 0:
        raise RuntimeError(
            f"GitHub did not download the expected asset {tag}/{asset_name}."
        )
    return path


def _validate_archive_metadata(
    path: Path, release_tag: str, asset_name: str
) -> SnapshotArchive | None:
    """Validate identity and return approved archives; return None for blocked evidence."""
    asset_match = ASSET_RE.fullmatch(asset_name)
    if not asset_match:
        raise ValueError(f"Unexpected raw archive asset name: {asset_name!r}")
    asset_run_id, asset_attempt = asset_match.groups()
    try:
        with ZipFile(path) as archive:
            bad_member = archive.testzip()
            if bad_member is not None:
                raise ValueError(
                    f"ZIP CRC validation failed for member {bad_member!r}."
                )
            if "bundle.json" not in archive.namelist():
                raise ValueError("Raw snapshot archive does not contain bundle.json.")
            bundle = json.loads(archive.read("bundle.json"))
    except (BadZipFile, OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Cannot inspect immutable snapshot archive {path}: {exc}"
        ) from exc
    if not isinstance(bundle, dict):
        raise ValueError("bundle.json must contain a JSON object.")
    if bundle.get("format_version") != 1 or bundle.get("snapshot_stage") != "raw":
        raise ValueError("Archive is not a supported raw snapshot bundle.")

    run_id = str(bundle.get("source_run_id", ""))
    attempt = str(bundle.get("source_run_attempt", "1"))
    commit = str(bundle.get("source_commit", ""))
    if (run_id, attempt) != (asset_run_id, asset_attempt):
        raise ValueError(
            "Raw archive asset name does not match bundle run/attempt identity."
        )
    if not SHA_RE.fullmatch(commit):
        raise ValueError("Raw archive does not contain a full source commit SHA.")

    extracted_at = _utc_datetime(bundle.get("extraction_started_at_utc", ""))
    expected_tag = f"toronto-raw-snapshot-archive-{extracted_at:%Y-%m}"
    if release_tag != expected_tag:
        raise ValueError(
            f"Archive month does not match UTC extraction timestamp: "
            f"tag={release_tag}, expected={expected_tag}."
        )

    sources = bundle.get("sources")
    if not isinstance(sources, dict):
        raise ValueError("Raw archive bundle sources must be a JSON object.")
    declared_complete = bundle.get("complete") is True
    declared_approved = bundle.get("approved_for_warehouse") is True
    if not declared_complete or not declared_approved:
        print(
            f"Skipping retained but warehouse-ineligible archive "
            f"{release_tag}/{asset_name}; it remains preserved as evidence."
        )
        return None
    missing_or_unapproved = [
        name
        for name in SOURCE_NAMES
        if not isinstance(sources.get(name), dict)
        or sources[name].get("warehouse_eligible") is not True
    ]
    if missing_or_unapproved:
        raise ValueError(
            "Bundle declares itself complete and approved but source summaries "
            f"are missing or ineligible: {missing_or_unapproved}"
        )
    quality_blocked = [
        name
        for name in SOURCE_NAMES
        if sources[name].get("validation_errors")
        or sources[name].get("warehouse_quality_blockers")
    ]
    if quality_blocked:
        raise ValueError(
            "Bundle claims warehouse approval but contains source validation or "
            f"quality blockers: {quality_blocked}"
        )

    return SnapshotArchive(
        release_tag=release_tag,
        asset_name=asset_name,
        path=path,
        bundle=bundle,
        extracted_at=extracted_at,
        run_id=run_id,
        attempt=attempt,
    )


def discover_archives(
    repository: str,
    destination: Path,
    *,
    exclude_snapshot_ids: set[str] | None = None,
    include_snapshot_ids: set[str] | None = None,
) -> list[SnapshotArchive]:
    """Download approved archives that still need processing, in extraction order.

    Completed identities can be skipped using the warehouse ledger. This keeps
    daily reconciliation from repeatedly downloading the entire permanent archive
    history. Explicitly included identities (for manual identity verification)
    are downloaded even when already complete.
    """
    discovered: list[SnapshotArchive] = []
    seen_ids: set[str] = set()
    excluded = exclude_snapshot_ids or set()
    included = include_snapshot_ids or set()
    assets = _monthly_release_assets(repository)

    for index, (tag, asset_name, _release_id) in enumerate(assets):
        identity = ASSET_RE.fullmatch(asset_name)
        if identity is None:
            continue
        run_id, attempt = identity.groups()
        snapshot_id = f"{run_id}:{attempt}"
        if snapshot_id in excluded and snapshot_id not in included:
            continue
        target = destination / f"archive-{index:06d}"
        path = _download_asset(repository, tag, asset_name, target)
        candidate = _validate_archive_metadata(path, tag, asset_name)
        if candidate is None:
            continue
        if candidate.snapshot_id in seen_ids:
            raise ValueError(
                f"Duplicate immutable archive identity found: {candidate.snapshot_id}"
            )
        seen_ids.add(candidate.snapshot_id)
        # The immutable Release bundle carries its own run/attempt/commit
        # provenance. Do not depend on short-lived Actions run metadata here:
        # permanent archives must remain replayable after run records expire.
        discovered.append(candidate)

    discovered.sort(key=lambda item: item.order)
    return discovered
