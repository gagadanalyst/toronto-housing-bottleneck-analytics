"""Versioned inventory for approved immutable Toronto snapshot archives."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import BadZipFile, ZipFile

INVENTORY_VERSION = 1
REPOSITORY = "gagadanalyst/toronto-housing-bottleneck-analytics"
CONTRACT_ID = "toronto-housing-snapshot-replay"
V1_TAG = re.compile(r"toronto-raw-snapshot-archive-\d{4}-\d{2}\Z")
V2_TAG = re.compile(r"toronto-raw-snapshot-v2-archive-\d{4}-\d{2}\Z")
V1_ASSET = re.compile(r"toronto-raw-snapshot-bundle-(\d+)-(\d+)\.zip\Z")
V2_ASSET = re.compile(
    r"toronto-raw-snapshot-v2-bundle-([A-Za-z0-9][A-Za-z0-9._-]{0,127})\.zip\Z"
)
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
COMMIT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z", re.IGNORECASE)
FIELDS = {
    "sequence",
    "snapshot_id",
    "format_version",
    "extracted_at_utc",
    "run_id",
    "run_attempt",
    "source_commit",
    "release_tag",
    "asset_name",
    "archive_size_bytes",
    "archive_sha256",
    "replay_contract",
}


def _asset_id(name: str) -> str | None:
    if match := V1_ASSET.fullmatch(name):
        return f"{match.group(1)}:{match.group(2)}"
    if match := V2_ASSET.fullmatch(name):
        return match.group(1)
    return None


def _utc(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an ISO-8601 UTC timestamp.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO-8601 UTC timestamp.") from exc
    offset = parsed.utcoffset()
    if parsed.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{label} must include a UTC timezone.")
    return parsed.astimezone(UTC)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _contract_ref(bundle: dict[str, Any], version: int) -> dict[str, Any] | None:
    has_contract = "replay_contract" in bundle
    has_digest = "replay_contract_sha256" in bundle
    if version == 1:
        if has_contract or has_digest:
            raise ValueError("V1 archive cannot declare a V2 replay contract.")
        return None
    if has_contract != has_digest:
        raise ValueError("V2 replay-contract fields are incomplete.")
    if not has_contract:  # Legacy V2 archive.
        return None
    contract, digest = bundle["replay_contract"], bundle["replay_contract_sha256"]
    if not isinstance(contract, dict) or contract.get("contract_id") != CONTRACT_ID:
        raise ValueError("V2 replay contract is missing or unsupported.")
    number = contract.get("contract_version")
    if type(number) is not int or number < 1:
        raise ValueError("V2 replay-contract version is invalid.")
    if not isinstance(digest, str) or not SHA256.fullmatch(digest):
        raise ValueError("V2 replay-contract SHA-256 is invalid.")
    canonical = json.dumps(
        contract, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    if hashlib.sha256(canonical).hexdigest() != digest:
        raise ValueError("V2 replay-contract checksum does not match its content.")
    return {"contract_id": CONTRACT_ID, "contract_version": number, "sha256": digest}


def record_from_archive(archive: Any, sequence: int) -> dict[str, Any]:
    """Create a canonical record from a validated SnapshotArchive."""
    path = Path(archive.path)
    if type(sequence) is not int or sequence < 1:
        raise ValueError("Inventory sequence must be a positive integer.")
    if not path.is_file() or path.stat().st_size < 1:
        raise ValueError(f"Snapshot archive is missing or empty: {path}.")
    digest = _sha256(path)
    if digest != archive.archive_sha256:
        raise ValueError(f"Snapshot archive changed after inspection: {path}.")

    sources = getattr(archive, "sources", None)
    if not isinstance(sources, dict) or set(sources) != {
        "active_permits",
        "cleared_permits",
    }:
        raise ValueError("Archive must contain both validated permit sources.")
    errors = [
        f"{name}: {source.error}"
        for name, source in sources.items()
        if getattr(source, "error", None)
    ]
    if errors:
        raise ValueError("Archive source validation failed: " + "; ".join(errors))

    version = archive.format_version
    if type(version) is not int or version not in (1, 2):
        raise ValueError("Snapshot format version must be 1 or 2.")
    if _asset_id(archive.asset_name) != archive.snapshot_id:
        raise ValueError("Archive asset name and snapshot identity disagree.")
    tag_pattern = V1_TAG if version == 1 else V2_TAG
    asset_pattern = V1_ASSET if version == 1 else V2_ASSET
    if not tag_pattern.fullmatch(archive.release_tag) or not asset_pattern.fullmatch(
        archive.asset_name
    ):
        raise ValueError("Archive format, asset name and Release tag disagree.")

    run_id = str(archive.run_id)
    attempt = archive.run_attempt
    if not run_id.isdecimal() or int(run_id) < 1:
        raise ValueError("Archive run ID must be a positive decimal string.")
    if type(attempt) is not int or attempt < 1:
        raise ValueError("Archive run attempt must be a positive integer.")
    if not COMMIT.fullmatch(str(archive.source_commit)):
        raise ValueError("Archive source commit must be a full Git SHA.")
    if (
        not isinstance(archive.extracted_at, datetime)
        or archive.extracted_at.tzinfo is None
        or archive.extracted_at.utcoffset() is None
    ):
        raise ValueError("Archive extraction timestamp must be timezone-aware.")
    extracted = archive.extracted_at.astimezone(UTC)
    if archive.release_tag[-7:] != extracted.strftime("%Y-%m"):
        raise ValueError("Archive Release month does not match extraction time.")
    if version == 1:
        match = V1_ASSET.fullmatch(archive.asset_name)
        if (match.group(1), match.group(2)) != (run_id, str(attempt)):
            raise ValueError("V1 asset name and GitHub run identity disagree.")

    return {
        "sequence": sequence,
        "snapshot_id": archive.snapshot_id,
        "format_version": version,
        "extracted_at_utc": extracted.isoformat().replace("+00:00", "Z"),
        "run_id": run_id,
        "run_attempt": attempt,
        "source_commit": str(archive.source_commit).lower(),
        "release_tag": archive.release_tag,
        "asset_name": archive.asset_name,
        "archive_size_bytes": path.stat().st_size,
        "archive_sha256": digest,
        "replay_contract": _contract_ref(archive.bundle, version),
    }


def _order(record: dict[str, Any]) -> tuple[datetime, int, int, str]:
    return (
        _utc(record["extracted_at_utc"], "extracted_at_utc"),
        int(record["run_id"]),
        record["run_attempt"],
        record["snapshot_id"],
    )


def _gh_json(*args: str) -> Any:
    result = subprocess.run(
        ["gh", "api", "--paginate", "--slurp", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        return json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise RuntimeError("GitHub CLI returned invalid JSON.") from exc


def _flatten_pages(value: Any) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    if isinstance(value, list):
        for item in value:
            result.extend(_flatten_pages(item))
    elif isinstance(value, dict):
        result.append(value)
    return result


def list_release_assets(repository: str) -> list[dict[str, Any]]:
    """List every approved V1/V2 ZIP with GitHub's size and SHA-256 digest."""
    releases = _flatten_pages(_gh_json(f"repos/{repository}/releases?per_page=100"))
    found: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_assets: set[tuple[str, str]] = set()

    for release in releases:
        tag = release.get("tag_name")
        if (
            release.get("draft") is True
            or release.get("prerelease") is True
            or not isinstance(tag, str)
            or not (V1_TAG.fullmatch(tag) or V2_TAG.fullmatch(tag))
        ):
            continue
        release_id = release.get("id")
        if type(release_id) is not int or release_id < 1:
            raise ValueError(f"Release {tag!r} has no valid numeric ID.")
        assets = _flatten_pages(
            _gh_json(f"repos/{repository}/releases/{release_id}/assets?per_page=100")
        )
        expected_pattern = V1_ASSET if V1_TAG.fullmatch(tag) else V2_ASSET
        for asset in assets:
            name = asset.get("name")
            if not isinstance(name, str):
                raise ValueError(f"Release {tag!r} contains an unnamed asset.")
            snapshot_id = _asset_id(name)
            if snapshot_id is None:
                if name.startswith(
                    ("toronto-raw-snapshot-bundle-", "toronto-raw-snapshot-v2-bundle-")
                ):
                    raise ValueError(f"Malformed snapshot archive asset: {tag}/{name}.")
                continue
            if not expected_pattern.fullmatch(name):
                raise ValueError(
                    f"Snapshot asset format does not match Release tag: {tag}/{name}."
                )
            size = asset.get("size")
            digest = asset.get("digest")
            digest_match = re.fullmatch(r"sha256:([0-9a-f]{64})", str(digest or ""))
            if type(size) is not int or size < 1 or size >= 2_147_483_648:
                raise ValueError(f"Release asset {tag}/{name} has an invalid size.")
            if not digest_match:
                raise ValueError(
                    f"Release asset {tag}/{name} has no valid SHA-256 digest."
                )
            if asset.get("state") not in (None, "uploaded"):
                raise ValueError(f"Release asset {tag}/{name} is not uploaded.")
            key = (tag, name)
            if key in seen_assets or snapshot_id in seen_ids:
                raise ValueError(
                    f"Duplicate immutable snapshot Release asset: {tag}/{name}."
                )
            seen_assets.add(key)
            seen_ids.add(snapshot_id)
            found.append(
                {
                    "release_tag": tag,
                    "asset_name": name,
                    "snapshot_id": snapshot_id,
                    "archive_size_bytes": size,
                    "archive_sha256": digest_match.group(1),
                }
            )

    return sorted(found, key=lambda item: (item["release_tag"], item["asset_name"]))


def _download_inspected_archives(
    repository: str, destination: Path, assets: list[dict[str, Any]]
) -> list[Any]:
    """Download only the enumerated approved assets and inspect their ZIPs."""
    from scripts.replay_archive import inspect_archive

    destination.mkdir(parents=True, exist_ok=True)
    archives = []
    for index, asset in enumerate(assets):
        tag = asset["release_tag"]
        name = asset["asset_name"]
        folder = destination / f"archive-{index:05d}"
        folder.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            [
                "gh",
                "release",
                "download",
                tag,
                "--repo",
                repository,
                "--pattern",
                name,
                "--dir",
                str(folder),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        path = folder / name
        if not path.is_file() or path.stat().st_size != asset["archive_size_bytes"]:
            raise ValueError(
                f"Downloaded archive size differs from Release metadata: {tag}/{name}."
            )
        archives.append(inspect_archive(path, tag, name, repository=repository))
    return sorted(archives, key=lambda item: item.order)


def validate_inventory(value: Any, repository: str | None = None) -> dict[str, Any]:
    """Validate version, record shape, uniqueness and canonical archive order."""
    if not isinstance(value, dict) or set(value) != {
        "inventory_version",
        "repository",
        "archives",
    }:
        raise ValueError("Snapshot inventory has an invalid top-level structure.")
    if type(value["inventory_version"]) is not int or value["inventory_version"] != 1:
        raise ValueError("Unsupported snapshot inventory version.")
    repo = value["repository"]
    if not isinstance(repo, str) or not re.fullmatch(r"[^/\s]+/[^/\s]+", repo):
        raise ValueError("Snapshot inventory repository is invalid.")
    if repository and repo.lower() != repository.lower():
        raise ValueError("Snapshot inventory belongs to a different repository.")
    archives = value["archives"]
    if not isinstance(archives, list) or not archives:
        raise ValueError("Snapshot inventory must contain at least one archive.")

    ids: set[str] = set()
    assets: set[tuple[str, str]] = set()
    previous = None
    for index, item in enumerate(archives, 1):
        if not isinstance(item, dict) or set(item) != FIELDS:
            raise ValueError(f"Inventory record {index} has invalid fields.")
        if type(item["sequence"]) is not int or item["sequence"] != index:
            raise ValueError("Inventory sequence must be contiguous from 1.")
        version = item["format_version"]
        if type(version) is not int or version not in (1, 2):
            raise ValueError("Inventory format_version must be 1 or 2.")
        name, tag = item["asset_name"], item["release_tag"]
        if (
            not isinstance(name, str)
            or not isinstance(tag, str)
            or _asset_id(name) != item["snapshot_id"]
        ):
            raise ValueError("Inventory asset name and snapshot ID disagree.")
        patterns = (V1_ASSET, V1_TAG) if version == 1 else (V2_ASSET, V2_TAG)
        if not patterns[0].fullmatch(name) or not patterns[1].fullmatch(tag):
            raise ValueError("Inventory archive format, name and Release tag disagree.")
        timestamp = _utc(item["extracted_at_utc"], "extracted_at_utc")
        if tag[-7:] != timestamp.strftime("%Y-%m"):
            raise ValueError("Inventory Release month does not match extraction time.")
        run_id, attempt = item["run_id"], item["run_attempt"]
        if not isinstance(run_id, str) or not run_id.isdecimal() or int(run_id) < 1:
            raise ValueError("Inventory run_id is invalid.")
        if type(attempt) is not int or attempt < 1:
            raise ValueError("Inventory run_attempt is invalid.")
        if version == 1:
            match = V1_ASSET.fullmatch(name)
            if (match.group(1), match.group(2)) != (run_id, str(attempt)):
                raise ValueError("V1 inventory asset and run identity disagree.")
        if not isinstance(item["source_commit"], str) or not COMMIT.fullmatch(
            item["source_commit"]
        ):
            raise ValueError("Inventory source commit is invalid.")
        if (
            type(item["archive_size_bytes"]) is not int
            or item["archive_size_bytes"] < 1
        ):
            raise ValueError("Inventory archive size is invalid.")
        if not isinstance(item["archive_sha256"], str) or not SHA256.fullmatch(
            item["archive_sha256"]
        ):
            raise ValueError("Inventory archive SHA-256 is invalid.")
        contract = item["replay_contract"]
        if contract is not None:
            if (
                version != 2
                or not isinstance(contract, dict)
                or set(contract) != {"contract_id", "contract_version", "sha256"}
            ):
                raise ValueError("Inventory replay-contract reference is invalid.")
            if contract["contract_id"] != CONTRACT_ID:
                raise ValueError("Inventory replay-contract ID is unsupported.")
            if (
                type(contract["contract_version"]) is not int
                or contract["contract_version"] < 1
            ):
                raise ValueError("Inventory replay-contract version is invalid.")
            if not isinstance(contract["sha256"], str) or not SHA256.fullmatch(
                contract["sha256"]
            ):
                raise ValueError("Inventory replay-contract checksum is invalid.")

        identity = item["snapshot_id"]
        asset_key = (tag, name)
        if identity in ids or asset_key in assets:
            raise ValueError(
                "Snapshot inventory contains duplicate archive identities."
            )
        ids.add(identity)
        assets.add(asset_key)
        current = _order(item)
        if previous is not None and current <= previous:
            raise ValueError("Snapshot inventory is not in strict archive order.")
        previous = current
    return value


def build_inventory(repository: str, archives: list[Any]) -> dict[str, Any]:
    ordered = sorted(archives, key=lambda item: item.order)
    result = {
        "inventory_version": INVENTORY_VERSION,
        "repository": repository,
        "archives": [record_from_archive(item, i) for i, item in enumerate(ordered, 1)],
    }
    return validate_inventory(result, repository)


def validate_inventory_assets(
    inventory: dict[str, Any],
    assets: list[dict[str, Any]],
    pending: list[dict[str, Any]] | None = None,
) -> None:
    """Match every approved Release asset exactly; optionally permit listed new uploads."""
    validate_inventory(inventory)
    expected = {
        (item["release_tag"], item["asset_name"]): item
        for item in inventory["archives"]
    }
    for item in pending or []:
        key = (item["release_tag"], item["asset_name"])
        current = expected.get(key)
        if current is not None and any(
            current[field] != item[field]
            for field in ("snapshot_id", "archive_size_bytes", "archive_sha256")
        ):
            raise ValueError(
                "New archive conflicts with its existing inventory record."
            )
        expected[key] = item

    actual = {}
    actual_ids: set[str] = set()
    for asset in assets:
        key = (asset["release_tag"], asset["asset_name"])
        if key in actual or asset["snapshot_id"] in actual_ids:
            raise ValueError("GitHub Release list contains duplicate archive assets.")
        actual[key] = asset
        actual_ids.add(asset["snapshot_id"])
    if set(actual) != set(expected):
        missing, extra = (
            sorted(set(expected) - set(actual)),
            sorted(set(actual) - set(expected)),
        )
        raise ValueError(
            f"Inventory/Release assets differ (missing={missing}, unindexed={extra})."
        )
    for key, item in expected.items():
        asset = actual[key]
        for field in ("snapshot_id", "archive_size_bytes", "archive_sha256"):
            if asset[field] != item[field]:
                raise ValueError(
                    f"Release asset metadata differs from inventory: {key}."
                )


def validate_inventory_archives(
    inventory: dict[str, Any], archives: list[Any], repository: str
) -> None:
    validate_inventory(inventory, repository)
    actual = build_inventory(repository, archives)
    if actual["archives"] != inventory["archives"]:
        raise ValueError(
            "Downloaded archive history does not exactly match snapshot inventory."
        )


def load_inventory(path: Path, repository: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Required snapshot inventory is missing: {path}.") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Snapshot inventory is not valid JSON: {path}.") from exc
    return validate_inventory(value, repository)


def write_inventory(path: Path, inventory: dict[str, Any], repository: str) -> None:
    path = Path(path)
    validate_inventory(inventory, repository)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(inventory, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def _archive_location(path: Path) -> tuple[str, str]:
    try:
        with ZipFile(path) as zipped:
            bundle = json.loads(zipped.read("bundle.json"))
    except (BadZipFile, KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Local archive has no valid bundle.json.") from exc
    if not isinstance(bundle, dict):
        raise ValueError("Local archive bundle.json must be an object.")
    version = bundle.get("format_version")
    started = _utc(bundle.get("extraction_started_at_utc"), "bundle start")
    if type(version) is not int or version not in (1, 2):
        raise ValueError(f"Unsupported snapshot format version: {version!r}.")
    prefix = (
        "toronto-raw-snapshot-archive-"
        if version == 1
        else "toronto-raw-snapshot-v2-archive-"
    )
    return f"{prefix}{started:%Y-%m}", path.name


def append_published_archive(
    archive_path: Path, inventory_path: Path, repository: str
) -> bool:
    """Append one published archive, requiring it to be the only unindexed Release asset."""
    from scripts.replay_archive import inspect_archive

    inventory = load_inventory(inventory_path, repository)
    archive_path = Path(archive_path)
    release_tag, asset_name = _archive_location(archive_path)
    archive = inspect_archive(
        archive_path, release_tag, asset_name, repository=repository
    )
    existing = next(
        (
            item
            for item in inventory["archives"]
            if item["snapshot_id"] == archive.snapshot_id
        ),
        None,
    )
    record = record_from_archive(
        archive, existing["sequence"] if existing else len(inventory["archives"]) + 1
    )
    validate_inventory_assets(
        inventory, list_release_assets(repository), pending=[record]
    )

    if existing:
        if existing != record:
            raise ValueError(
                "Archive already exists in inventory with different metadata."
            )
        return False
    if _order(record) <= _order(inventory["archives"][-1]):
        raise ValueError("New snapshot does not follow the latest inventory entry.")
    inventory["archives"].append(record)
    validate_inventory(inventory, repository)
    write_inventory(inventory_path, inventory, repository)
    return True


def bootstrap_inventory(
    output: Path, repository: str, *, expected_count: int | None = None
) -> dict[str, Any]:
    """Build the initial inventory once; an existing baseline is never replaced."""
    output = Path(output)
    if output.exists():
        raise ValueError(f"Inventory already exists: {output}; refusing to overwrite.")
    assets = list_release_assets(repository)
    if expected_count is not None and len(assets) != expected_count:
        raise ValueError(
            f"Expected {expected_count} approved archives; found {len(assets)}."
        )
    if not assets:
        raise ValueError("No approved V1/V2 snapshot archives were found.")
    with tempfile.TemporaryDirectory(prefix="toronto-snapshot-inventory-") as folder:
        archives = _download_inspected_archives(repository, Path(folder), assets)
        inventory = build_inventory(repository, archives)
        validate_inventory_assets(inventory, assets)
    write_inventory(output, inventory, repository)
    return inventory


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    bootstrap = commands.add_parser("bootstrap")
    bootstrap.add_argument("--repository", default=REPOSITORY)
    bootstrap.add_argument(
        "--output", type=Path, default=Path("snapshot_inventory.json")
    )
    bootstrap.add_argument("--expected-count", type=int)
    append = commands.add_parser("append")
    append.add_argument("--repository", default=REPOSITORY)
    append.add_argument(
        "--inventory", type=Path, default=Path("snapshot_inventory.json")
    )
    append.add_argument("--archive", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "bootstrap":
            inventory = bootstrap_inventory(
                args.output,
                args.repository,
                expected_count=args.expected_count,
            )
            v1 = sum(item["format_version"] == 1 for item in inventory["archives"])
            v2 = len(inventory["archives"]) - v1
            print(
                f"Created {args.output}: {len(inventory['archives'])} archives (V1={v1}, V2={v2})."
            )
        else:
            changed = append_published_archive(
                args.archive, args.inventory, args.repository
            )
            print(
                "Inventory updated."
                if changed
                else "Archive already indexed; no change."
            )
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"Snapshot inventory failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
