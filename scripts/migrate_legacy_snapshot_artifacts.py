"""Migrate legacy Actions snapshot-bundle artifacts into immutable monthly Releases.

The default run list is the 28 previously identified successful historical CD runs.
This command is dry-run by default. Add --publish to create immutable Release assets.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZIP_STORED, BadZipFile, ZipFile

from pyarrow import parquet as pq

from toronto_housing_ingestion.config import SourceConfig, load_config

EXPECTED_LEGACY_RUN_IDS = (
    "40470002594",
    "40482942648",
    "40491722684",
    "40508242800",
    "40513793300",
    "40533706926",
    "40567655707",
    "40607541510",
    "40622186735",
    "40630228426",
    "40638789933",
    "40641089502",
    "40668146616",
    "40679228717",
    "40788712364",
    "40796993767",
    "40820837442",
    "40848285474",
    "41034120228",
    "41187888283",
    "41295484744",
    "41396374837",
    "41403055348",
    "41440929410",
    "41533746699",
    "41628672679",
    "41824174680",
    "42022028625",
)
EXPECTED_LEGACY_RUN_ATTEMPTS = {
    "40491722684": "2",
    "40679228717": "4",
}
SOURCE_NAMES = ("active_permits", "cleared_permits")
ARTIFACT_RE = re.compile(r"toronto-snapshot-bundle-(\d+)\Z")
ASSET_RE = re.compile(r"toronto-raw-snapshot-bundle-(\d+)-(\d+)\.zip\Z")
MAX_ASSET_BYTES = 2_147_483_648


def _run_json(*args: str) -> Any:
    result = subprocess.run(
        ["gh", "api", *args], check=True, capture_output=True, text=True
    )
    try:
        return json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"GitHub API returned invalid JSON for {args[-1]!r}."
        ) from exc


def _run(*args: str) -> None:
    subprocess.run(["gh", *args], check=True)


def _flatten_pages(value: Any) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    if isinstance(value, list):
        for item in value:
            output.extend(_flatten_pages(item))
    elif isinstance(value, dict):
        if isinstance(value.get("artifacts"), list):
            output.extend(item for item in value["artifacts"] if isinstance(item, dict))
        else:
            output.append(value)
    return output


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _timestamp(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO-8601 timestamp.") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must include a timezone.")
    return parsed.astimezone(UTC)


def _read_json(payload: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON.") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain a JSON object.")
    return value


def migrate_legacy_bundle(
    legacy_path: Path,
    output_path: Path,
    *,
    run_id: str,
    attempt: str,
    commit: str,
    sources: dict[str, SourceConfig],
    previous_row_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Convert the old loaded-snapshot bundle to the current raw archive format.

    Valid sources are independently retained. Invalid, missing or quality-blocked
    sources are represented in bundle metadata and remain ineligible for replay.
    """
    if (
        not run_id.isdigit()
        or not attempt.isdigit()
        or not re.fullmatch(r"[0-9a-f]{40}", commit)
    ):
        raise ValueError("Legacy run identity, attempt or commit is invalid.")
    previous_row_counts = previous_row_counts if previous_row_counts is not None else {}

    with ZipFile(legacy_path) as legacy:
        # Do not call ZipFile.testzip() globally here: one corrupt source member
        # must not prevent independent retention of the other valid source.
        names = set(legacy.namelist())
        if "bundle.json" not in names:
            raise ValueError("Legacy snapshot bundle has no bundle.json.")
        old_bundle = _read_json(legacy.read("bundle.json"), "legacy bundle.json")
        if str(old_bundle.get("source_run_id", "")) != run_id:
            raise ValueError(
                "Legacy bundle run ID does not match its Actions artifact."
            )
        if old_bundle.get("source_commit") != commit:
            raise ValueError("Legacy bundle commit does not match its Actions run.")

        started = _timestamp(
            old_bundle.get("extraction_started_at_utc", ""),
            "legacy bundle extraction_started_at_utc",
        )
        old_summaries = old_bundle.get("sources")
        if not isinstance(old_summaries, dict):
            # Bundle-level source metadata can be incomplete while one or both
            # source members are still recoverable. Preserve their bytes as
            # unapproved evidence rather than discarding the entire archive.
            old_summaries = {}

        output_sources: dict[str, dict[str, Any]] = {}
        output_members: dict[str, bytes] = {}
        failures: dict[str, list[str]] = {}
        source_quality_blockers: dict[str, list[str]] = {}
        missing_sources: list[str] = []

        for name in SOURCE_NAMES:
            source = sources.get(name)
            summary = old_summaries.get(name)
            parquet_member = f"{name}/data.parquet"
            manifest_member = f"{name}/manifest.json"
            source_errors: list[str] = []
            blockers: list[str] = []
            if source is None:
                raise ValueError(
                    f"Current configuration is missing required source {name!r}."
                )
            if not isinstance(summary, dict):
                source_errors.append(
                    "legacy bundle source summary is missing or invalid"
                )
                try:
                    orphan_bytes = (
                        legacy.read(parquet_member) if parquet_member in names else None
                    )
                except (BadZipFile, RuntimeError, OSError) as exc:
                    orphan_bytes = None
                    source_errors.append(
                        f"legacy Parquet member could not be read: {type(exc).__name__}: {exc}"
                    )
                if orphan_bytes is None:
                    missing_sources.append(name)
                    source_errors.append("legacy Parquet member is missing")
                    orphan_digest = None
                    orphan_rows = None
                    orphan_columns: list[str] = []
                else:
                    # Retain readable Parquet even without bundle summary metadata;
                    # absent provenance makes it warehouse-ineligible.
                    output_members[parquet_member] = orphan_bytes
                    orphan_digest = _sha256_bytes(orphan_bytes)
                    try:
                        orphan_parquet = pq.ParquetFile(io.BytesIO(orphan_bytes))
                        orphan_rows = int(orphan_parquet.metadata.num_rows)
                        orphan_columns = list(orphan_parquet.schema_arrow.names)
                    except Exception as exc:
                        orphan_rows = None
                        orphan_columns = []
                        source_errors.append(
                            f"legacy Parquet validation failed: {type(exc).__name__}: {exc}"
                        )
                manifest = {
                    "source_name": name,
                    "source_run_id": run_id,
                    "source_run_attempt": attempt,
                    "source_commit": commit,
                    "extracted_at_utc": started.isoformat(),
                    "sha256": orphan_digest,
                    "row_count": orphan_rows,
                    "columns": orphan_columns,
                    "status": "extracted_unvalidated",
                    "warehouse_status": "blocked",
                    "warehouse_eligible": False,
                    "warehouse_quality_blockers": [],
                    "validation_errors": source_errors,
                    "raw_path": None,
                }
                output_members[manifest_member] = (
                    json.dumps(manifest, indent=2, sort_keys=True).encode() + b"\n"
                )
                output_sources[name] = {
                    "snapshot": parquet_member if orphan_bytes is not None else None,
                    "manifest": manifest_member,
                    "sha256": orphan_digest,
                    "row_count": orphan_rows,
                    "columns": orphan_columns,
                    "required_columns": list(source.required_columns),
                    "warehouse_since": source.warehouse_since,
                    "warehouse_date_column": source.warehouse_date_column,
                    "resource_id": None,
                    "extracted_at_utc": started.isoformat(),
                    "warehouse_eligible": False,
                    "warehouse_quality_blockers": [],
                    "validation_errors": source_errors,
                    "has_snapshot": orphan_bytes is not None,
                }
                failures[name] = source_errors
                continue

            try:
                parquet_bytes = (
                    legacy.read(parquet_member) if parquet_member in names else None
                )
            except (BadZipFile, RuntimeError, OSError) as exc:
                parquet_bytes = None
                source_errors.append(
                    f"legacy Parquet member could not be read: {type(exc).__name__}: {exc}"
                )
            try:
                manifest_bytes = (
                    legacy.read(manifest_member) if manifest_member in names else None
                )
            except (BadZipFile, RuntimeError, OSError) as exc:
                manifest_bytes = None
                source_errors.append(
                    f"legacy manifest member could not be read: {type(exc).__name__}: {exc}"
                )
            manifest: dict[str, Any]

            if manifest_bytes is not None:
                try:
                    manifest = _read_json(manifest_bytes, manifest_member)
                except ValueError as exc:
                    manifest = {"source_name": name, "legacy_manifest_error": str(exc)}
                    source_errors.append(str(exc))
            else:
                manifest = {"source_name": name}
                source_errors.append("legacy manifest is missing")

            if parquet_bytes is None:
                source_errors.append("legacy Parquet member is missing")
                missing_sources.append(name)
                actual_digest = None
                row_count = None
                columns: list[str] = []
            else:
                actual_digest = _sha256_bytes(parquet_bytes)
                if actual_digest != summary.get(
                    "sha256"
                ) or actual_digest != manifest.get("sha256"):
                    source_errors.append(
                        "legacy Parquet SHA-256 does not match bundle/manifest metadata"
                    )
                try:
                    parquet_file = pq.ParquetFile(io.BytesIO(parquet_bytes))
                    row_count = int(parquet_file.metadata.num_rows)
                    columns = list(parquet_file.schema_arrow.names)
                    if row_count < 1:
                        source_errors.append("legacy Parquet snapshot is empty")
                    if row_count != summary.get(
                        "row_count"
                    ) or row_count != manifest.get("row_count"):
                        source_errors.append(
                            "legacy Parquet row count does not match metadata"
                        )
                    if sorted(columns) != sorted(summary.get("columns", [])):
                        source_errors.append(
                            "legacy Parquet schema does not match bundle metadata"
                        )
                    if sorted(columns) != sorted(manifest.get("columns", [])):
                        source_errors.append(
                            "legacy Parquet schema does not match source manifest"
                        )
                except Exception as exc:
                    row_count = None
                    columns = []
                    source_errors.append(
                        f"legacy Parquet validation failed: {type(exc).__name__}: {exc}"
                    )

                output_members[parquet_member] = parquet_bytes

            if (
                manifest.get("status") != "success"
                or manifest.get("warehouse_status") != "loaded"
            ):
                blockers.append("legacy_snapshot_was_not_successfully_warehouse_loaded")
            if source.warehouse_strategy.lower() != "scd2" or not source.warehouse_load:
                blockers.append("source_configuration_is_not_an_enabled_SCD2_load")
            if (
                source.min_row_count is not None
                and row_count is not None
                and row_count < source.min_row_count
            ):
                blockers.append(
                    f"below_minimum_row_count:{row_count}<{source.min_row_count}"
                )
            previous_rows = previous_row_counts.get(name)
            if (
                previous_rows
                and row_count is not None
                and source.max_row_change_percent is not None
            ):
                change_percent = abs(row_count - previous_rows) / previous_rows * 100
                if change_percent > source.max_row_change_percent:
                    blockers.append(
                        "row_count_change_exceeds_configured_limit:"
                        f"{change_percent:.2f}%>{source.max_row_change_percent:.2f}%"
                    )
            if manifest.get("warehouse_since") != source.warehouse_since:
                blockers.append("warehouse_since_differs_from_current_configuration")
            if manifest.get("warehouse_date_column") != source.warehouse_date_column:
                blockers.append(
                    "warehouse_date_column_differs_from_current_configuration"
                )

            missing_required = sorted(
                column
                for column in source.required_columns
                if column.lower() not in {item.lower() for item in columns}
            )
            if missing_required:
                blockers.append(
                    "missing_required_columns:" + ",".join(missing_required)
                )
            warnings = manifest.get("quality_warnings", [])
            if isinstance(warnings, list):
                invalid_key_warnings = [
                    str(item.get("type"))
                    for item in warnings
                    if isinstance(item, dict)
                    and item.get("type") in {"missing_key", "missing_key_columns"}
                ]
                if invalid_key_warnings:
                    blockers.append("legacy_snapshot_has_invalid_business_keys")
            elif warnings:
                blockers.append("legacy_quality_warnings_metadata_is_invalid")

            try:
                extracted_at = _timestamp(
                    manifest.get(
                        "extracted_at_utc", summary.get("extracted_at_utc", "")
                    ),
                    f"{name} extracted_at_utc",
                )
                summary_extracted_at = _timestamp(
                    summary.get("extracted_at_utc", ""),
                    f"{name} bundle summary extracted_at_utc",
                )
                if extracted_at != summary_extracted_at:
                    source_errors.append(
                        "source manifest and bundle summary extraction timestamps disagree"
                    )
                if extracted_at < started:
                    source_errors.append(
                        "source extraction timestamp predates bundle extraction marker"
                    )
            except ValueError as exc:
                extracted_at = started
                source_errors.append(str(exc))

            eligible = bool(
                parquet_bytes is not None
                and row_count is not None
                and row_count > 0
                and not source_errors
                and not blockers
            )
            if eligible and row_count is not None:
                # Only a warehouse-approved snapshot may advance the next
                # historical row-count comparison baseline.
                previous_row_counts[name] = row_count
            normalized_manifest = {
                **manifest,
                "source_name": name,
                "source_run_id": run_id,
                "source_run_attempt": attempt,
                "source_commit": commit,
                "extracted_at_utc": extracted_at.isoformat(),
                "status": "success" if not source_errors else "extracted_unvalidated",
                "warehouse_status": "not_loaded" if eligible else "blocked",
                "warehouse_eligible": eligible,
                "warehouse_quality_blockers": sorted(set(blockers)),
                "validation_errors": source_errors,
                "legacy_warehouse_status": manifest.get("warehouse_status"),
                "raw_path": None,
            }
            manifest_output = (
                json.dumps(normalized_manifest, indent=2, sort_keys=True).encode()
                + b"\n"
            )
            output_members[manifest_member] = manifest_output

            output_sources[name] = {
                "snapshot": parquet_member if parquet_bytes is not None else None,
                "manifest": manifest_member,
                "sha256": actual_digest,
                "row_count": row_count,
                "columns": columns,
                "required_columns": list(source.required_columns),
                "warehouse_since": source.warehouse_since,
                "warehouse_date_column": source.warehouse_date_column,
                "resource_id": manifest.get("resource_id"),
                "extracted_at_utc": extracted_at.isoformat(),
                "warehouse_eligible": eligible,
                "warehouse_quality_blockers": sorted(set(blockers)),
                "validation_errors": source_errors,
                "has_snapshot": parquet_bytes is not None,
            }
            if source_errors:
                failures[name] = source_errors
            if blockers:
                source_quality_blockers[name] = sorted(set(blockers))

        complete = (
            set(output_sources) == set(SOURCE_NAMES)
            and all(output_sources[name]["has_snapshot"] for name in SOURCE_NAMES)
            and not any(failures.values())
        )
        approved = complete and all(
            output_sources[name]["warehouse_eligible"] for name in SOURCE_NAMES
        )
        bundle = {
            "format_version": 1,
            "snapshot_stage": "raw",
            "complete": complete,
            "approved_for_warehouse": approved,
            "missing_sources": missing_sources,
            "source_failures": failures,
            "source_quality_blockers": source_quality_blockers,
            "source_run_id": run_id,
            "source_run_attempt": attempt,
            "source_commit": commit,
            "extraction_started_at_utc": started.isoformat(),
            "sources": output_sources,
            "migrated_from": "legacy_actions_artifact",
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with ZipFile(output_path, "w", compression=ZIP_STORED) as output:
            output.writestr(
                "bundle.json", json.dumps(bundle, indent=2, sort_keys=True) + "\n"
            )
            for member, value in sorted(output_members.items()):
                output.writestr(member, value)
        with ZipFile(output_path) as check:
            bad_member = check.testzip()
            if bad_member is not None:
                raise ValueError(
                    f"Migrated bundle failed ZIP integrity for {bad_member!r}."
                )
        return bundle


def _release_tag(extraction_started_at_utc: str) -> str:
    timestamp = _timestamp(extraction_started_at_utc, "extraction_started_at_utc")
    return f"toronto-raw-snapshot-archive-{timestamp:%Y-%m}"


def _release_assets(repository: str, tag: str) -> tuple[int, list[dict[str, Any]]]:
    release = _run_json(f"repos/{repository}/releases/tags/{tag}")
    release_id = int(release["id"])
    pages = _run_json(
        "--paginate",
        "--slurp",
        f"repos/{repository}/releases/{release_id}/assets?per_page=100",
    )
    assets = _flatten_pages(pages)
    return release_id, assets


def _publish_immutable(
    repository: str, tag: str, asset_path: Path, asset_name: str
) -> None:
    if asset_path.stat().st_size <= 0 or asset_path.stat().st_size >= MAX_ASSET_BYTES:
        raise ValueError(
            f"Archive asset size is invalid or exceeds 2 GiB: {asset_path.stat().st_size}"
        )

    release_result = subprocess.run(
        ["gh", "release", "view", tag, "--repo", repository],
        capture_output=True,
        text=True,
    )
    if release_result.returncode != 0:
        _run(
            "release",
            "create",
            tag,
            "--repo",
            repository,
            "--title",
            f"Toronto raw snapshot archive {tag[-7:]}",
            "--notes",
            "Immutable historical and current raw permit snapshots. Assets are never overwritten or automatically pruned.",
        )
    try:
        _release_id, assets = _release_assets(repository, tag)
    except (subprocess.CalledProcessError, KeyError, ValueError) as exc:
        raise RuntimeError(
            f"Cannot inspect Release assets for {tag}; refusing to publish blindly."
        ) from exc

    matches = [item for item in assets if item.get("name") == asset_name]
    if len(matches) > 1:
        raise RuntimeError(
            f"Release {tag} contains duplicate asset names: {asset_name}"
        )
    if matches:
        with tempfile.TemporaryDirectory(prefix="toronto-legacy-asset-check-") as temp:
            _run(
                "release",
                "download",
                tag,
                "--repo",
                repository,
                "--pattern",
                asset_name,
                "--dir",
                temp,
            )
            existing = Path(temp) / asset_name
            if (
                not existing.is_file()
                or existing.stat().st_size != asset_path.stat().st_size
                or _sha256_file(existing) != _sha256_file(asset_path)
            ):
                raise RuntimeError(
                    f"Existing immutable asset differs from migration bytes: {tag}/{asset_name}"
                )
        print(f"Already present byte-for-byte: {tag}/{asset_name}")
        return
    if len(assets) >= 1000:
        raise RuntimeError(
            f"Release asset limit reached for {tag}; no assets were removed."
        )
    _run("release", "upload", tag, str(asset_path), "--repo", repository)
    with tempfile.TemporaryDirectory(prefix="toronto-legacy-asset-verify-") as temp:
        _run(
            "release",
            "download",
            tag,
            "--repo",
            repository,
            "--pattern",
            asset_name,
            "--dir",
            temp,
        )
        downloaded = Path(temp) / asset_name
        if not downloaded.is_file() or _sha256_file(downloaded) != _sha256_file(
            asset_path
        ):
            raise RuntimeError(
                f"Published immutable asset failed download verification: {tag}/{asset_name}"
            )
    print(f"Published and verified: {tag}/{asset_name}")


def migrate_run(
    repository: str,
    run_id: str,
    sources: dict[str, SourceConfig],
    *,
    publish: bool,
    previous_row_counts: dict[str, int] | None = None,
) -> tuple[bool, str]:
    artifacts_value = _run_json(
        "--paginate", "--slurp", f"repos/{repository}/actions/artifacts?per_page=100"
    )
    artifacts = _flatten_pages(artifacts_value)
    artifact_name = f"toronto-snapshot-bundle-{run_id}"
    matches = [item for item in artifacts if item.get("name") == artifact_name]
    if len(matches) != 1:
        return (
            False,
            f"expected one legacy artifact {artifact_name}; found {len(matches)}",
        )
    artifact = matches[0]
    if artifact.get("expired") is True:
        return False, f"legacy artifact {artifact_name} has expired"

    run = _run_json(f"repos/{repository}/actions/runs/{run_id}")
    if (
        str(run.get("id")) != run_id
        or run.get("status") != "completed"
        or run.get("conclusion") != "success"
        or run.get("head_branch") != "main"
        or run.get("event") != "workflow_run"
    ):
        return (
            False,
            f"run {run_id} is not a successful completed main-branch CD workflow_run",
        )
    attempt = str(run.get("run_attempt", ""))
    commit = str(run.get("head_sha", ""))
    if not attempt.isdigit() or not re.fullmatch(r"[0-9a-f]{40}", commit):
        return False, f"run {run_id} has invalid attempt or source commit metadata"

    with tempfile.TemporaryDirectory(prefix=f"toronto-legacy-{run_id}-") as temp:
        root = Path(temp)
        download_dir = root / "artifact"
        download_dir.mkdir()
        _run(
            "run",
            "download",
            run_id,
            "--repo",
            repository,
            "--name",
            artifact_name,
            "--dir",
            str(download_dir),
        )
        candidates = sorted(download_dir.rglob("*.zip"))
        if len(candidates) != 1:
            return (
                False,
                f"artifact {artifact_name} should contain one bundle ZIP; found {len(candidates)}",
            )
        migrated = root / f"toronto-raw-snapshot-bundle-{run_id}-{attempt}.zip"
        bundle = migrate_legacy_bundle(
            candidates[0],
            migrated,
            run_id=run_id,
            attempt=attempt,
            commit=commit,
            sources=sources,
            previous_row_counts=previous_row_counts,
        )
        tag = _release_tag(bundle["extraction_started_at_utc"])
        asset_name = migrated.name
        if publish:
            _publish_immutable(repository, tag, migrated, asset_name)
        state = (
            "approved"
            if bundle["approved_for_warehouse"]
            else "retained but warehouse-blocked"
        )
        return True, f"{run_id}:{attempt} -> {tag}/{asset_name} ({state})"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("open_data_config.yml"))
    parser.add_argument(
        "--run-id",
        action="append",
        help="Migrate only this legacy run ID; repeat as needed.",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Actually publish immutable monthly Release assets. Default is dry run.",
    )
    args = parser.parse_args()

    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if not repository:
        raise ValueError("GITHUB_REPOSITORY is required.")
    if not os.environ.get("GH_TOKEN") and not os.environ.get("GITHUB_TOKEN"):
        raise ValueError(
            "GH_TOKEN or GITHUB_TOKEN is required for Actions artifact discovery."
        )

    app = load_config(args.config)
    sources = {source.name: source for source in app.sources}
    run_ids = args.run_id or list(EXPECTED_LEGACY_RUN_IDS)
    if len(run_ids) != len(set(run_ids)) or any(
        not value.isdigit() for value in run_ids
    ):
        raise ValueError("Run IDs must be unique and contain digits only.")

    failures: list[str] = []
    successes: list[str] = []
    previous_row_counts: dict[str, int] = {}
    # GitHub run IDs increase with workflow creation order. Sorting makes the
    # historical comparison deterministic even when --run-id values are supplied
    # in a different order.
    run_ids = sorted(run_ids, key=int)
    for run_id in run_ids:
        try:
            ok, message = migrate_run(
                repository,
                run_id,
                sources,
                publish=args.publish,
                previous_row_counts=previous_row_counts,
            )
        except Exception as exc:
            ok = False
            message = f"{run_id}: {type(exc).__name__}: {exc}"
        print(message)
        (successes if ok else failures).append(message)

    mode = "published" if args.publish else "dry-run prepared"
    print(
        f"Legacy artifact migration {mode}: {len(successes)} succeeded, {len(failures)} failed."
    )
    if failures:
        print("Missing/failed historical artifacts require investigation:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
