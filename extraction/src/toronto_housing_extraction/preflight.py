"""Conservative no-change preflight for paired Toronto source snapshots."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .ckan_client import TorontoCKANClient, is_reliable_datastore_fingerprint
from .config import load_config
from .release import (
    FINGERPRINT_ASSET_PREFIX,
    SOURCE_NAMES,
    V2_ASSET_PREFIX,
    V2_RELEASE_PREFIX,
    _run_gh,
)

FINGERPRINT_FIELDS = (
    "fingerprint_version",
    "resource_id",
    "datastore_cache_last_update",
    "resource_last_modified",
    "datastore_total",
    "total_was_estimated",
    "schema_sha256",
)
SHA256_PATTERN = re.compile(r"sha256:([0-9a-f]{64})\Z")


@dataclass(frozen=True)
class PreflightResult:
    """Decision and explanation from a conservative source-change check."""

    no_change: bool
    reason: str


def _pages(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise RuntimeError("GitHub returned an invalid Release list.")
    releases: list[dict[str, Any]] = []
    for page in value:
        if isinstance(page, list):
            releases.extend(item for item in page if isinstance(item, dict))
        elif isinstance(page, dict):
            releases.append(page)
        else:
            raise RuntimeError("GitHub returned an invalid Release page.")
    return releases


def _latest_bundle(
    repository: str, gh: str
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    endpoint = f"repos/{repository}/releases?per_page=100"
    response = _run_gh(["api", "--paginate", "--slurp", endpoint], gh)
    releases = _pages(json.loads(response or "[]"))
    candidates: list[tuple[str, str, dict[str, Any], dict[str, Any]]] = []

    for release in releases:
        tag = release.get("tag_name")
        if (
            not isinstance(tag, str)
            or not tag.startswith(V2_RELEASE_PREFIX)
            or release.get("draft") is True
            or release.get("prerelease") is True
        ):
            continue
        assets = release.get("assets", [])
        if not isinstance(assets, list):
            continue
        for asset in assets:
            if not isinstance(asset, dict) or asset.get("state") != "uploaded":
                continue
            name = asset.get("name")
            if (
                isinstance(name, str)
                and name.startswith(V2_ASSET_PREFIX)
                and name.endswith(".zip")
            ):
                created = str(asset.get("created_at") or "")
                published = str(release.get("published_at") or "")
                candidates.append((created, published, release, asset))

    if not candidates:
        return None

    _, _, release, asset = max(candidates, key=lambda item: (item[0], item[1]))
    return release, asset


def _download_asset(repository: str, tag: str, name: str, gh: str) -> bytes:
    with tempfile.TemporaryDirectory(prefix="toronto-v2-preflight-") as directory:
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
        path = Path(directory) / name
        if not path.is_file() or not path.stat().st_size:
            raise RuntimeError(f"GitHub did not download Release asset {name!r}.")
        return path.read_bytes()


def _asset_sha256(asset: dict[str, Any]) -> str | None:
    digest = asset.get("digest")
    if not isinstance(digest, str):
        return None
    match = SHA256_PATTERN.fullmatch(digest)
    return match.group(1) if match else None


def _load_latest_index(repository: str, gh: str) -> tuple[dict[str, Any] | None, str]:
    latest = _latest_bundle(repository, gh)
    if latest is None:
        return None, "no V2 snapshot archive exists yet"

    release, archive_asset = latest
    archive_name = archive_asset.get("name")
    archive_digest = _asset_sha256(archive_asset)
    archive_size = archive_asset.get("size")
    tag = release.get("tag_name")

    if (
        not isinstance(archive_name, str)
        or not isinstance(tag, str)
        or archive_digest is None
        or type(archive_size) is not int
    ):
        return None, "latest archive lacks a verifiable SHA-256 digest or size"

    snapshot_id = archive_name.removeprefix(V2_ASSET_PREFIX).removesuffix(".zip")
    if not snapshot_id:
        return None, "latest archive has an invalid snapshot identifier"
    expected_name = f"{FINGERPRINT_ASSET_PREFIX}{snapshot_id}.json"
    assets = release.get("assets", [])
    sidecars = [
        item
        for item in assets
        if isinstance(item, dict) and item.get("name") == expected_name
    ]
    if len(sidecars) != 1:
        return (
            None,
            "latest archive has no unique fingerprint sidecar; extraction required",
        )

    sidecar_asset = sidecars[0]
    sidecar_digest = _asset_sha256(sidecar_asset)
    sidecar_size = sidecar_asset.get("size")
    if (
        sidecar_asset.get("state") != "uploaded"
        or sidecar_digest is None
        or type(sidecar_size) is not int
    ):
        return None, "fingerprint sidecar lacks a verifiable SHA-256 digest or size"

    sidecar_bytes = _download_asset(repository, tag, expected_name, gh)
    if (
        len(sidecar_bytes) != sidecar_size
        or hashlib.sha256(sidecar_bytes).hexdigest() != sidecar_digest
    ):
        return None, "fingerprint sidecar integrity check failed"

    try:
        index = json.loads(sidecar_bytes)
    except UnicodeDecodeError, json.JSONDecodeError:
        return None, "fingerprint sidecar is not valid JSON"

    if not isinstance(index, dict) or index.get("index_version") != 1:
        return None, "fingerprint sidecar version is unsupported"
    if (
        index.get("snapshot_id") != snapshot_id
        or index.get("release_tag") != tag
        or index.get("archive_asset_name") != archive_name
        or index.get("archive_sha256") != archive_digest
        or index.get("archive_size_bytes") != archive_size
    ):
        return None, "fingerprint sidecar does not describe the latest archive"

    sources = index.get("sources")
    if not isinstance(sources, dict) or set(sources) != set(SOURCE_NAMES):
        return None, "fingerprint sidecar does not contain both configured sources"
    return index, "latest archive fingerprint sidecar verified"


def _valid_comparable_fingerprint(value: Any) -> bool:
    if not isinstance(value, dict) or type(value.get("fingerprint_version")) is not int:
        return False
    if value["fingerprint_version"] != 1:
        return False

    markers = (
        value.get("datastore_cache_last_update"),
        value.get("resource_last_modified"),
    )
    has_valid_marker = any(
        isinstance(marker, str) and bool(marker.strip()) for marker in markers
    )

    estimated = value.get("total_was_estimated")

    return bool(
        has_valid_marker
        and isinstance(value.get("resource_id"), str)
        and bool(value["resource_id"].strip())
        and type(value.get("datastore_total")) is int
        and "total_was_estimated" in value
        and (estimated is None or isinstance(estimated, bool))
        and isinstance(value.get("schema_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", value["schema_sha256"])
    )


def comparable_fingerprint(value: Any) -> dict[str, Any] | None:
    """Select source-controlled fields; ignore package-level diagnostic timestamps."""
    if not _valid_comparable_fingerprint(value):
        return None
    return {field: value.get(field) for field in FINGERPRINT_FIELDS}


def source_fingerprints_match(
    current: dict[str, dict[str, Any]],
    archived: dict[str, dict[str, Any]],
) -> bool:
    """Return true only when both reliable DataStore source fingerprints match."""
    if set(current) != set(SOURCE_NAMES) or set(archived) != set(SOURCE_NAMES):
        return False

    for source_name in SOURCE_NAMES:
        live = current[source_name]
        previous = archived[source_name]
        if (
            live.get("fingerprint_reliable") is not True
            or previous.get("fingerprint_reliable") is not True
            or live.get("extraction_method") != "datastore"
            or previous.get("extraction_method") != "datastore"
        ):
            return False
        live_fingerprint = comparable_fingerprint(live.get("source_fingerprint"))
        old_fingerprint = comparable_fingerprint(previous.get("source_fingerprint"))
        if live_fingerprint is None or old_fingerprint is None:
            return False
        if (
            live.get("resource_id") != live_fingerprint["resource_id"]
            or previous.get("resource_id") != old_fingerprint["resource_id"]
        ):
            return False
        if live_fingerprint != old_fingerprint:
            return False
    return True


def _resource_markers_unchanged(before: dict[str, Any], after: dict[str, Any]) -> bool:
    """Check that a CKAN resource retained its identity and source markers."""
    return (
        after.get("id") == before.get("id")
        and after.get("datastore_cache_last_update")
        == before.get("datastore_cache_last_update")
        and after.get("last_modified") == before.get("last_modified")
    )


def _current_source_fingerprints(config_path: Path) -> dict[str, dict[str, Any]]:
    config = load_config(config_path)
    if set(config.sources) != set(SOURCE_NAMES):
        raise RuntimeError(
            "No-change detection requires the Active/Cleared source pair."
        )

    current: dict[str, dict[str, Any]] = {}
    resources_before: dict[str, dict[str, Any]] = {}

    with TorontoCKANClient(
        config.ckan.base_url,
        config.ckan.timeout_seconds,
        config.ckan.retries,
    ) as client:
        for source_name, source in config.sources.items():
            package = client.package_show(source.dataset_slug)
            resource = client.select_resource(package)

            if resource.get("datastore_active") is not True or not resource.get("id"):
                raise RuntimeError(
                    f"{source_name}: current source is not an active DataStore."
                )

            resource_id = str(resource["id"])
            details = client.action("resource_show", id=resource_id)
            if not isinstance(details, dict):
                raise RuntimeError(
                    f"{source_name}: resource metadata response is invalid."
                )

            resource = {**resource, **details}
            resources_before[source_name] = resource

            probe = client.probe_datastore(resource_id)
            fingerprint = client.source_fingerprint(
                resource,
                probe,
                package.get("metadata_modified"),
            )

            current[source_name] = {
                "resource_id": resource.get("id"),
                "extraction_method": "datastore",
                "source_fingerprint": fingerprint,
                "fingerprint_reliable": is_reliable_datastore_fingerprint(fingerprint),
            }

        # Recheck both resources after both probes. A refresh during the
        # paired check must cause extraction rather than authorize a skip.
        for source_name in SOURCE_NAMES:
            before = resources_before[source_name]
            after = client.action("resource_show", id=before["id"])

            if not isinstance(after, dict) or not _resource_markers_unchanged(
                before, after
            ):
                raise RuntimeError(
                    f"{source_name}: source changed during no-change preflight."
                )

    return current


def check_no_change(
    *, config_path: Path, repository: str, gh_executable: str = "gh"
) -> PreflightResult:
    """Compare live metadata with the latest verified V2 sidecar."""
    index, reason = _load_latest_index(repository, gh_executable)
    if index is None:
        return PreflightResult(False, reason)
    current = _current_source_fingerprints(config_path)
    if source_fingerprints_match(current, index["sources"]):
        return PreflightResult(
            True,
            "both Active and Cleared source fingerprints match the latest archive",
        )
    unreliable = [
        name
        for name in SOURCE_NAMES
        if current.get(name, {}).get("fingerprint_reliable") is not True
        or index["sources"].get(name, {}).get("fingerprint_reliable") is not True
    ]
    if unreliable:
        return PreflightResult(
            False,
            "one or more source fingerprints are not reliable: "
            + ", ".join(unreliable),
        )
    return PreflightResult(False, "one or more source fingerprints changed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/sources.yml"),
        help="Path to the extraction source configuration.",
    )
    parser.add_argument(
        "--repository",
        default=os.environ.get("GITHUB_REPOSITORY"),
        help="GitHub owner/name repository containing the V2 Releases.",
    )
    parser.add_argument("--gh", default="gh", help="GitHub CLI executable.")
    parser.add_argument(
        "--github-output",
        type=Path,
        default=Path(os.environ["GITHUB_OUTPUT"])
        if os.environ.get("GITHUB_OUTPUT")
        else None,
        help="Optional GitHub Actions output file.",
    )
    args = parser.parse_args()
    if not args.repository or args.repository.count("/") != 1:
        print(
            "toronto-check-snapshot: repository owner/name is required",
            file=sys.stderr,
        )
        return 2
    try:
        result = check_no_change(
            config_path=args.config,
            repository=args.repository,
            gh_executable=args.gh,
        )
    except Exception as exc:
        # A failed preflight must never suppress extraction.
        result = PreflightResult(
            False, f"preflight failed; extraction required: {type(exc).__name__}: {exc}"
        )
    value = "true" if result.no_change else "false"
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write(f"no_change={value}\n")
    status = "Unchanged" if result.no_change else "Extraction required"
    print(f"{status}: {result.reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
