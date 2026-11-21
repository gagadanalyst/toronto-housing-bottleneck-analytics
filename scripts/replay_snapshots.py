"""Discover immutable V1/V2 Releases and replay them into one candidate backend."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import UTC, date
from pathlib import Path
from typing import Any

from scripts import replay_warehouse_ledger
from scripts.replay_archive import (
    SOURCE_NAMES,
    SnapshotArchive,
    inspect_archive,
    validate_warehouse_eligibility,
)
from scripts.replay_ledger import ReplayLedger
from scripts.replay_targets import (
    DuckDBReplayTarget,
    SnowflakeReplayTarget,
    replay_backend,
)
from scripts.snapshot_inventory import (
    list_release_assets,
    load_inventory,
    validate_inventory_archives,
    validate_inventory_assets,
)
from scripts.source_replay_policy import (
    load_source_replay_policy,
    validate_source_replay_policy,
)

V1_TAG = re.compile(r"toronto-raw-snapshot-archive-\d{4}-\d{2}\Z")
V2_TAG = re.compile(r"toronto-raw-snapshot-v2-archive-\d{4}-\d{2}\Z")
V1_ASSET = re.compile(r"toronto-raw-snapshot-bundle-(\d+)-(\d+)\.zip\Z")
V2_ASSET = re.compile(
    r"toronto-raw-snapshot-v2-bundle-([A-Za-z0-9][A-Za-z0-9._-]{0,127})\.zip\Z"
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
    flattened: list[dict[str, Any]] = []
    if isinstance(value, list):
        for item in value:
            flattened.extend(_flatten_pages(item))
    elif isinstance(value, dict):
        flattened.append(value)
    return flattened


def _asset_snapshot_id(name: str) -> str | None:
    if match := V1_ASSET.fullmatch(name):
        return f"{match.group(1)}:{match.group(2)}"
    if match := V2_ASSET.fullmatch(name):
        return match.group(1)
    return None


def discover_archives(
    repository: str,
    destination: Path,
    *,
    exclude_snapshot_ids: set[str] | None = None,
) -> list[SnapshotArchive]:
    """Download and validate pending V1/V2 archive assets in Release order."""
    excluded = exclude_snapshot_ids or set()
    releases = _flatten_pages(_gh_json(f"repos/{repository}/releases?per_page=100"))
    candidates: list[tuple[str, str, int]] = []

    for release in releases:
        tag = str(release.get("tag_name", ""))
        if (
            release.get("draft") is True
            or release.get("prerelease") is True
            or not (V1_TAG.fullmatch(tag) or V2_TAG.fullmatch(tag))
        ):
            continue
        release_id = release.get("id")
        if not isinstance(release_id, int):
            raise ValueError(f"Release {tag!r} has no valid numeric ID.")
        assets = _flatten_pages(
            _gh_json(f"repos/{repository}/releases/{release_id}/assets?per_page=100")
        )
        for asset in assets:
            name = str(asset.get("name", ""))
            snapshot_id = _asset_snapshot_id(name)
            if snapshot_id is None or snapshot_id in excluded:
                continue
            size = asset.get("size")
            if not isinstance(size, int) or size <= 0 or size >= 2_147_483_648:
                raise ValueError(f"Release asset {tag}/{name} has an invalid size.")
            candidates.append((tag, name, release_id))

    destination.mkdir(parents=True, exist_ok=True)
    archives: list[SnapshotArchive] = []
    seen: set[str] = set()
    for index, (tag, name, _release_id) in enumerate(candidates):
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
        if not path.is_file() or path.stat().st_size == 0:
            raise RuntimeError(f"GitHub did not download {tag}/{name}.")
        archive = inspect_archive(path, tag, name, repository=repository)
        if archive.snapshot_id in seen:
            raise ValueError(
                f"Duplicate immutable snapshot identity: {archive.snapshot_id}."
            )
        seen.add(archive.snapshot_id)
        archives.append(archive)

    return sorted(archives, key=lambda item: item.order)


def _source_configs(config_path: Path) -> dict[str, Any]:
    from toronto_housing_ingestion.config import load_config

    app = load_config(config_path)
    sources = {source.name: source for source in app.sources if source.warehouse_load}
    expected = {"active_permits", "cleared_permits"}
    if set(sources) != expected:
        raise ValueError("Configuration must enable Active and Cleared permit sources.")
    for source in sources.values():
        if source.warehouse_strategy != "scd2":
            raise ValueError(f"{source.name} must use the SCD2 strategy for replay.")
    return sources


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("limit must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("limit must be a positive integer")
    return parsed


def _iso_date(value: str | None, label: str) -> date | None:
    if value is None or value == "":
        return None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"{label} must use YYYY-MM-DD format.")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} is not a valid calendar date.") from exc


def select_archives(
    archives: list[SnapshotArchive],
    *,
    mode: str,
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int | None = None,
) -> tuple[list[SnapshotArchive], str]:
    """Select a stable chronological rebuild scope before applying retry skips.

    Archive timestamps have already been validated as timezone-aware by the
    archive reader. Date filters are interpreted as inclusive UTC calendar days.
    """
    has_selection = bool(from_date or to_date or limit is not None)
    if mode == "incremental":
        if has_selection:
            raise ValueError(
                "Date and N-snapshot selection are available only in rebuild mode."
            )
        return sorted(archives, key=lambda item: item.order), "incremental"
    if mode != "rebuild":
        raise ValueError(f"Unsupported replay mode: {mode!r}.")

    lower = _iso_date(from_date, "--from-date")
    upper = _iso_date(to_date, "--to-date")
    if lower and upper and lower > upper:
        raise ValueError("--from-date must be on or before --to-date.")
    if limit is not None and limit < 1:
        raise ValueError("--limit must be a positive integer.")

    selected = sorted(archives, key=lambda item: item.order)
    if lower:
        selected = [
            item
            for item in selected
            if item.extracted_at.astimezone(UTC).date() >= lower
        ]
    if upper:
        selected = [
            item
            for item in selected
            if item.extracted_at.astimezone(UTC).date() <= upper
        ]
    if limit is not None:
        # Select the newest N snapshots in the requested date window, then keep
        # them oldest-first so SCD2 boundaries are reconstructed chronologically.
        # Apply this before retry/idempotency skips so retries cannot expand N.
        selected = selected[-limit:]
    if not selected:
        raise ValueError("The requested rebuild scope contains no archived snapshots.")
    return selected, "partial" if has_selection else "full"


def _write_github_outputs(values: dict[str, str]) -> None:
    """Publish step outputs when running under GitHub Actions."""
    output_path = os.getenv("GITHUB_OUTPUT")
    if not output_path:
        return
    with Path(output_path).open("a", encoding="utf-8") as stream:
        for key, value in values.items():
            if "\n" in key or "\r" in key or "\n" in value or "\r" in value:
                raise ValueError("GitHub step outputs must be single-line values.")
            stream.write(f"{key}={value}\n")


def _validate_selection_request(args: argparse.Namespace) -> None:
    """Reject invalid mode/scope requests before opening a candidate or ledger."""
    if args.mode not in {"incremental", "rebuild"}:
        raise ValueError(f"Unsupported replay mode: {args.mode!r}.")
    has_selection = bool(args.from_date or args.to_date or args.limit is not None)
    if args.mode == "incremental" and has_selection:
        raise ValueError(
            "Date and N-snapshot selection are available only in rebuild mode."
        )
    if args.mode == "rebuild":
        lower = _iso_date(args.from_date, "--from-date")
        upper = _iso_date(args.to_date, "--to-date")
        if lower and upper and lower > upper:
            raise ValueError("--from-date must be on or before --to-date.")
        if args.limit is not None and args.limit < 1:
            raise ValueError("--limit must be a positive integer.")
        if args.backend == "duckdb" and args.database is None:
            raise ValueError(
                "DuckDB rebuild requires an explicit --database candidate path."
            )
        if args.backend == "snowflake" and not args.schema:
            raise ValueError(
                "Snowflake rebuild requires an explicit --schema candidate schema."
            )
    if args.mode == "incremental" and (
        not args.baseline_snapshot_id or args.baseline_snapshot_count is None
    ):
        raise ValueError(
            "Incremental replay requires --baseline-snapshot-id and "
            "--baseline-snapshot-count for canonical baseline validation."
        )


def _build_target(args: argparse.Namespace) -> Any:
    """Construct the selected backend target with the requested safety mode."""
    if args.mode not in {"incremental", "rebuild"}:
        raise ValueError(f"Unsupported replay mode: {args.mode!r}.")
    if args.backend == "duckdb":
        return DuckDBReplayTarget(
            args.database,
            dataset_name=args.dataset,
            mode=args.mode,
        )
    if args.backend == "snowflake":
        if not args.snowflake_credentials:
            raise ValueError(
                "SNOWFLAKE_DLT_CONNECTION is required for Snowflake replay."
            )
        return SnowflakeReplayTarget(
            args.snowflake_credentials,
            args.schema,
            mode=args.mode,
        )
    raise ValueError(f"Unsupported replay backend: {args.backend!r}.")


def _validate_incremental_quality_baseline(
    archive: SnapshotArchive, record: dict[str, Any]
) -> dict[str, Any]:
    """Verify the downloaded quality-reference archive against successful ledger evidence."""
    snapshot_id = str(record.get("snapshot_id", ""))
    if not snapshot_id or archive.snapshot_id != snapshot_id:
        raise RuntimeError(
            "Downloaded Mode 1 quality baseline does not match the latest "
            "successfully loaded snapshot in the replay ledger."
        )

    identity_fields = (
        "format_version",
        "run_id",
        "run_attempt",
        "source_commit",
        "archive_sha256",
        "release_tag",
        "asset_name",
    )
    integer_fields = {"format_version", "run_attempt"}

    for field in identity_fields:
        actual = getattr(archive, field)
        expected = record.get(field)
        if field in integer_fields:
            matches = int(actual) == int(expected)
        else:
            matches = str(actual) == str(expected)
        if not matches:
            raise RuntimeError(
                f"Mode 1 quality baseline {snapshot_id} disagrees with "
                f"the replay ledger field {field!r}."
            )

    source_records = record.get("source_records")
    if (
        not isinstance(source_records, dict)
        or set(source_records) != set(SOURCE_NAMES)
        or set(archive.sources) != set(SOURCE_NAMES)
    ):
        raise RuntimeError(
            f"Mode 1 quality baseline {snapshot_id} does not contain "
            "a complete Active/Cleared source pair."
        )

    for source_name in SOURCE_NAMES:
        source_record = source_records[source_name]
        source = archive.sources[source_name]

        if source_record.get("status") != "succeeded":
            raise RuntimeError(
                f"Mode 1 quality baseline {snapshot_id}/{source_name} "
                "was not successfully loaded."
            )

        try:
            expected_timestamp = round(float(source_record["order_timestamp"]), 6)
            actual_timestamp = round(source.extracted_at.timestamp(), 6)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError(
                f"Mode 1 quality baseline {snapshot_id}/{source_name} "
                "has invalid source timestamp evidence."
            ) from exc

        if actual_timestamp != expected_timestamp:
            raise RuntimeError(
                f"Mode 1 quality baseline {snapshot_id}/{source_name} "
                "does not match its persisted source extraction timestamp."
            )

    return record



def _latest_successful_source_records(
    records: list[dict[str, Any]],
    source_exclusions: dict[tuple[str, str], dict[str, str]],
) -> dict[str, dict[str, Any]]:
    """Choose the newest successful, non-excluded observation per source."""
    latest: dict[str, dict[str, Any]] = {}
    latest_order: dict[str, tuple[float, int, int, str]] = {}

    for record in records:
        if not isinstance(record, dict):
            raise RuntimeError("Mode 1 source ledger contains an invalid record.")

        source_name = record.get("source_name")
        snapshot_id = record.get("snapshot_id")
        if source_name not in SOURCE_NAMES:
            raise RuntimeError(
                f"Mode 1 source ledger has an unsupported source: {source_name!r}."
            )
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise RuntimeError("Mode 1 source ledger contains an invalid snapshot ID.")

        if record.get("status") != "succeeded":
            continue
        if (snapshot_id, source_name) in source_exclusions:
            continue

        try:
            order = (
                float(record["order_timestamp"]),
                int(record["run_id"]),
                int(record["run_attempt"]),
                snapshot_id,
            )
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError(
                "Mode 1 source ledger contains invalid ordering metadata for "
                f"{source_name}/{snapshot_id}."
            ) from exc

        if source_name not in latest_order or order > latest_order[source_name]:
            latest[source_name] = dict(record)
            latest_order[source_name] = order

    missing = set(SOURCE_NAMES) - set(latest)
    if missing:
        raise RuntimeError(
            "Mode 1 has no successful, non-excluded quality baseline for: "
            + ", ".join(sorted(missing))
        )

    return latest


def _validate_incremental_source_quality_baseline(
    archive: SnapshotArchive,
    source_name: str,
    record: dict[str, Any],
) -> None:
    """Verify one source's reference archive against its successful ledger row."""
    if source_name not in SOURCE_NAMES:
        raise ValueError(f"Unsupported quality-baseline source: {source_name!r}.")

    if archive.snapshot_id != record.get("snapshot_id"):
        raise RuntimeError(
            f"Mode 1 {source_name} quality archive does not match its ledger record."
        )
    if record.get("source_name") != source_name:
        raise RuntimeError(
            f"Mode 1 {source_name} quality record has a mismatched source identity."
        )
    if record.get("status") != "succeeded":
        raise RuntimeError(
            f"Mode 1 {source_name}/{archive.snapshot_id} quality reference "
            "was not successfully loaded."
        )

    identity_fields = (
        "format_version",
        "run_id",
        "run_attempt",
        "source_commit",
        "archive_sha256",
        "release_tag",
        "asset_name",
    )
    integer_fields = {"format_version", "run_attempt"}

    for field in identity_fields:
        actual = getattr(archive, field)
        expected = record.get(field)
        if field in integer_fields:
            matches = int(actual) == int(expected)
        else:
            matches = str(actual) == str(expected)
        if not matches:
            raise RuntimeError(
                f"Mode 1 {source_name} quality reference {archive.snapshot_id} "
                f"disagrees with its ledger field {field!r}."
            )

    source = archive.sources.get(source_name)
    if source is None or source.error:
        detail = source.error if source is not None else "source is missing"
        raise RuntimeError(
            f"Mode 1 {source_name}/{archive.snapshot_id} quality reference "
            f"is invalid: {detail}"
        )
    if (
        type(source.row_count) is not int
        or source.row_count < 1
    ):
        raise RuntimeError(
            f"Mode 1 {source_name}/{archive.snapshot_id} quality reference "
            "has an invalid row count."
        )

    try:
        expected_timestamp = round(float(record["order_timestamp"]), 6)
        actual_timestamp = round(source.extracted_at.timestamp(), 6)
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError(
            f"Mode 1 {source_name}/{archive.snapshot_id} quality reference "
            "has invalid extraction timestamp evidence."
        ) from exc

    if actual_timestamp != expected_timestamp:
        raise RuntimeError(
            f"Mode 1 {source_name}/{archive.snapshot_id} quality reference "
            "does not match its persisted source extraction timestamp."
        )


def _validate_full_history_inventory(
    args: argparse.Namespace,
    source_exclusions: dict[tuple[str, str], dict[str, str]] | None = None,
) -> list[SnapshotArchive]:
    """Verify the complete Release set and every archive before opening a candidate."""
    if source_exclusions is None:
        policy_path = Path(
            getattr(args, "source_policy", Path("source_replay_policy.json"))
        )
        source_exclusions = load_source_replay_policy(policy_path)

    inventory = load_inventory(args.inventory, args.repository)
    assets = list_release_assets(args.repository)
    validate_inventory_assets(inventory, assets)
    archives = discover_archives(
        args.repository,
        args.archive_dir,
        exclude_snapshot_ids=set(),
    )
    validate_inventory_archives(inventory, archives, args.repository)

    # Require every policy entry to identify an archive in the complete
    # immutable inventory, and verify its archive SHA-256.
    validate_source_replay_policy(
        archives,
        source_exclusions,
        require_all=True,
    )

    # Reject poor-quality source history before either backend candidate is opened.
    validate_warehouse_eligibility(
        archives,
        source_exclusions=source_exclusions,
    )
    return archives


def run(args: argparse.Namespace) -> dict[str, Any]:
    # Validate scope and source quality before opening a backend candidate.
    _validate_selection_request(args)
    source_policy_path = Path(
        getattr(args, "source_policy", Path("source_replay_policy.json"))
    )
    source_exclusions = load_source_replay_policy(source_policy_path)

    prevalidated_rebuild_archives = (
        _validate_full_history_inventory(args, source_exclusions)
        if args.mode == "rebuild"
        else None
    )

    ledger = ReplayLedger(args.ledger)
    run_completed = ledger.completed_snapshot_ids(args.backend)
    baseline_preflight = None

    if args.mode == "incremental":
        baseline_record = ledger.latest_successful_snapshot(args.backend)
        if baseline_record is None:
            raise RuntimeError(
                "Mode 1 incremental replay has no successfully loaded "
                "Active/Cleared archive pair to use as its quality baseline."
            )

        baseline_snapshot_id = str(baseline_record["snapshot_id"])

        # The newest fully completed bundle remains the canonical Mode 1
        # baseline, but each source can have a different last valid observation.
        source_ledger_records = ledger.snapshot_records(args.backend)
        source_baseline_records = _latest_successful_source_records(
            source_ledger_records,
            source_exclusions,
        )
        source_baseline_ids = {
            record["snapshot_id"] for record in source_baseline_records.values()
        }
        reference_ids = {baseline_snapshot_id, *source_baseline_ids}

        archives_with_baseline = discover_archives(
            args.repository,
            args.archive_dir,
            # Re-fetch the common bundle baseline and each source-specific
            # reference. Other fully completed snapshots remain excluded.
            exclude_snapshot_ids=run_completed - reference_ids,
        )
        validate_source_replay_policy(
            archives_with_baseline,
            source_exclusions,
        )

        baseline_matches = [
            archive
            for archive in archives_with_baseline
            if archive.snapshot_id == baseline_snapshot_id
        ]
        if len(baseline_matches) != 1:
            raise RuntimeError(
                f"Latest successful Mode 1 baseline archive "
                f"{baseline_snapshot_id!r} must be present exactly once in "
                f"GitHub Releases; found {len(baseline_matches)}."
            )

        baseline_archive = baseline_matches[0]
        _validate_incremental_quality_baseline(baseline_archive, baseline_record)

        source_baseline_archives: dict[str, SnapshotArchive] = {}
        initial_source_baselines: dict[str, tuple[int, str]] = {}

        for source_name in SOURCE_NAMES:
            source_record = source_baseline_records[source_name]
            source_snapshot_id = source_record["snapshot_id"]
            matches = [
                archive
                for archive in archives_with_baseline
                if archive.snapshot_id == source_snapshot_id
            ]
            if len(matches) != 1:
                raise RuntimeError(
                    f"Mode 1 {source_name} quality-reference archive "
                    f"{source_snapshot_id!r} must be present exactly once in "
                    f"GitHub Releases; found {len(matches)}."
                )

            source_archive = matches[0]
            _validate_incremental_source_quality_baseline(
                source_archive,
                source_name,
                source_record,
            )
            source_baseline_archives[source_name] = source_archive
            initial_source_baselines[source_name] = (
                source_archive.sources[source_name].row_count,
                source_snapshot_id,
            )

        # A successful source stream can be ahead of the latest fully
        # completed bundle. Such observations are already in the candidate:
        # keep their sibling pending, but do not re-evaluate the loaded side.
        completed_source_observations = {
            (record["snapshot_id"], record["source_name"])
            for record in source_ledger_records
            if record.get("status") == "succeeded"
        }

        pending_archives = [
            archive
            for archive in archives_with_baseline
            if archive.snapshot_id != baseline_snapshot_id
            and archive.snapshot_id not in run_completed
        ]
        older_pending = [
            archive.snapshot_id
            for archive in pending_archives
            if archive.order <= baseline_archive.order
        ]
        if older_pending:
            raise RuntimeError(
                "Mode 1 found pending archives that do not follow the latest "
                "successfully loaded bundle baseline: "
                + ", ".join(sorted(older_pending))
            )

        # Do not seed a source from a later successful observation if an
        # earlier non-excluded observation for that source is still pending.
        for source_name in SOURCE_NAMES:
            reference = source_baseline_records[source_name]
            source_reference_order = (
                round(float(reference["order_timestamp"]), 6),
                int(reference["run_id"]),
                int(reference["run_attempt"]),
                reference["snapshot_id"],
            )

            unresolved_older = [
                archive.snapshot_id
                for archive in pending_archives
                if (
                    round(
                        archive.sources[source_name].extracted_at.timestamp(), 6
                    ),
                    int(archive.run_id),
                    int(archive.run_attempt),
                    archive.snapshot_id,
                ) < source_reference_order
                and (archive.snapshot_id, source_name)
                not in completed_source_observations
                and (archive.snapshot_id, source_name) not in source_exclusions
            ]
            if unresolved_older:
                raise RuntimeError(
                    f"Mode 1 {source_name} has pending observations older than "
                    f"its selected quality baseline "
                    f"{source_baseline_records[source_name]['snapshot_id']}: "
                    + ", ".join(sorted(unresolved_older))
                )

        if pending_archives:
            validate_warehouse_eligibility(
                pending_archives,
                source_exclusions=source_exclusions,
                initial_source_baselines=initial_source_baselines,
                completed_source_observations=completed_source_observations,
            )

        archives_discovered = pending_archives
        quality_preflight = {
            "status": "passed",
            "basis": (
                "latest_successful_non_excluded_source_observation_plus_pending_archives"
            ),
            "baseline_snapshot_id": baseline_snapshot_id,
            "baseline_archive_sha256": baseline_archive.archive_sha256,
            "source_baselines": {
                source_name: {
                    "snapshot_id": source_baseline_archives[source_name].snapshot_id,
                    "archive_sha256": source_baseline_archives[
                        source_name
                    ].archive_sha256,
                    "row_count": source_baseline_archives[
                        source_name
                    ].sources[source_name].row_count,
                }
                for source_name in SOURCE_NAMES
            },
            "completed_source_observations_skipped": [
                {
                    "snapshot_id": snapshot_id,
                    "source_name": source_name,
                }
                for snapshot_id, source_name in sorted(
                    completed_source_observations
                    & {
                        (archive.snapshot_id, source_name)
                        for archive in pending_archives
                        for source_name in SOURCE_NAMES
                    }
                )
            ],
            "snapshot_ids_checked": [
                archive.snapshot_id for archive in pending_archives
            ],
        }
        selected_archives, history_scope = select_archives(
            archives_discovered,
            mode=args.mode,
        )
    else:
        if prevalidated_rebuild_archives is None:
            raise RuntimeError("Full-history inventory verification did not run.")
        archives_discovered = prevalidated_rebuild_archives
        selected_archives, history_scope = select_archives(
            archives_discovered,
            mode=args.mode,
            from_date=args.from_date,
            to_date=args.to_date,
            limit=args.limit,
        )
        quality_preflight = {
            "status": "passed",
            "basis": "full_history_inventory",
            "baseline_snapshot_id": None,
            "snapshot_ids_checked": [
                archive.snapshot_id for archive in archives_discovered
            ],
        }

    # Both Mode 1 and Mode 2 reach this point only after source preflight.
    source_configs = _source_configs(args.config)
    target = _build_target(args)
    target_completed = target.completed_snapshot_ids()

    if args.mode == "incremental":
        # An exclusion cannot retroactively remove an observation already
        # loaded into this candidate. A fresh full-history rebuild is required.
        for (snapshot_id, source_name) in source_exclusions:
            if target.snapshot_status(snapshot_id, source_name) == "succeeded":
                raise RuntimeError(
                    "Mode 1 cannot apply a source exclusion retroactively to "
                    f"{snapshot_id}/{source_name}: the candidate already records "
                    "that source as succeeded. Perform a fresh full-history "
                    "rebuild before resuming incremental replay."
                )

        if target_completed != run_completed:
            provenance_only = sorted(run_completed - target_completed)
            candidate_only = sorted(target_completed - run_completed)
            raise RuntimeError(
                "Mode 1 provenance and candidate completion ledgers disagree; "
                "refusing to use an uncertain quality baseline. "
                f"provenance_only={provenance_only}, "
                f"candidate_only={candidate_only}."
            )

        baseline_preflight = replay_warehouse_ledger.validate_canonical_baseline(
            target,
            args.baseline_snapshot_id,
            args.baseline_snapshot_count,
        )

    pending_before_load = sum(
        archive.snapshot_id not in (target_completed | run_completed)
        for archive in selected_archives
    )

    generation_id = None
    if args.mode == "rebuild":
        generation_id = _generation_id(args)
        candidate = (
            str(args.database) if args.backend == "duckdb" else f"TORONTO.{args.schema}"
        )
        ledger.start_generation(
            args.backend,
            generation_id,
            mode=args.mode,
            history_scope=history_scope,
            selected_snapshot_count=len(selected_archives),
            candidate=candidate,
        )

    try:
        report = replay_backend(
            selected_archives,
            source_configs=source_configs,
            target=target,
            ledger_path=args.ledger,
            staging_root=args.staging_dir,
            source_exclusions=source_exclusions,
        )
        report.update(
            {
                "mode": args.mode,
                "baseline_preflight": baseline_preflight,
                "warehouse_quality_preflight": quality_preflight,
                "history_scope": history_scope,
                "full_history": history_scope == "full",
                "history_scope_basis": (
                    "versioned_inventory_verified_v1_v2_release_archives"
                    if history_scope == "full"
                    else (
                        "pending_incremental_backlog"
                        if history_scope == "incremental"
                        else "explicit_rebuild_selection"
                    )
                ),
                "explicit_promotion_required": True,
                "warehouse_input_staging": (
                    "required" if args.mode == "incremental" else "not_used"
                ),
                "snapshot_count_discovered": len(archives_discovered),
                "snapshot_count_selected": len(selected_archives),
                "snapshot_count_pending_before_load": pending_before_load,
                "snapshots": [
                    {
                        "snapshot_id": archive.snapshot_id,
                        "format_version": archive.format_version,
                        "extracted_at_utc": archive.extracted_at.isoformat(),
                        "source_commit": archive.source_commit,
                        "run_id": archive.run_id,
                        "run_attempt": archive.run_attempt,
                        "release_tag": archive.release_tag,
                        "asset_name": archive.asset_name,
                        "archive_sha256": archive.archive_sha256,
                    }
                    for archive in selected_archives
                ],
                "selection": {
                    "from_date": args.from_date or None,
                    "to_date": args.to_date or None,
                    "limit": args.limit,
                    "limit_semantics": "most_recent_n_then_replay_chronologically",
                    "ordering": "extracted_at_utc_ascending",
                    "limit_applied_before_retry_skips": True,
                },
                "ledger_path": str(args.ledger),
                # Includes selected archives from previous attempts, not just this
                # invocation.
                "ledger_entries": _report_ledger_entries(ledger, target, args.backend),
                "candidate": (
                    str(args.database)
                    if args.backend == "duckdb"
                    else f"TORONTO.{args.schema}"
                ),
            }
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        pending = bool(args.mode == "incremental" and pending_before_load > 0)
        _write_github_outputs(
            {
                "has_pending_archives": "true" if pending else "false",
                "history_scope": history_scope,
                "snapshot_count_selected": str(len(selected_archives)),
            }
        )
        return report
    except Exception as exc:
        if generation_id is not None:
            ledger.finish_generation(
                args.backend,
                generation_id,
                "FAILED",
                error=f"{type(exc).__name__}: {exc}",
            )
        raise


def _report_ledger_entries(
    ledger: ReplayLedger, target: Any, backend: str
) -> list[dict[str, Any]]:
    """Combine run-ledger provenance with persisted candidate evidence."""
    provenance_records = ledger.snapshot_records(backend)
    if not provenance_records:
        return []

    native_records = {
        (record["snapshot_id"], record["source_name"]): record
        for record in replay_warehouse_ledger.snapshot_records(target)
    }

    entries = []
    for record in provenance_records:
        key = (record["snapshot_id"], record["source_name"])
        native = native_records.get(key)
        if native is None:
            raise RuntimeError(f"Candidate ledger is missing {key[0]}/{key[1]}.")

        entries.append(
            {
                **record,
                "warehouse_status": native["status"],
                "quarantined_row_count": native["quarantined_row_count"],
                "quarantined_bad_row_count": native["quarantined_bad_row_count"],
                "quarantined_business_key_count": native[
                    "quarantined_business_key_count"
                ],
                "quarantine_sha256": native["quarantine_sha256"],
            }
        )

    return entries


def _generation_id(args: argparse.Namespace) -> str:
    explicit = getattr(args, "generation_id", None)
    if explicit:
        return str(explicit)

    run_id = os.getenv("GITHUB_RUN_ID")
    if run_id:
        return f"{run_id}:{os.getenv('GITHUB_RUN_ATTEMPT', '1')}"

    from uuid import uuid4

    return f"local:{uuid4().hex}"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline-snapshot-id",
        help="Final snapshot ID of the approved canonical baseline.",
    )
    parser.add_argument(
        "--baseline-snapshot-count",
        type=_positive_int,
        help="Expected number of snapshots through the approved baseline ID.",
    )
    parser.add_argument(
        "--mode",
        choices=("incremental", "rebuild"),
        default="rebuild",
        help=(
            "incremental stages new archives before applying SCD2 to a "
            "production-derived candidate; rebuild writes directly to an isolated "
            "historical reconstruction target without an extra warehouse input stage"
        ),
    )
    parser.add_argument("--backend", choices=("duckdb", "snowflake"), required=True)
    parser.add_argument(
        "--repository", default="gagadanalyst/toronto-housing-bottleneck-analytics"
    )
    parser.add_argument(
        "--inventory",
        type=Path,
        default=Path("snapshot_inventory.json"),
        help="Versioned inventory required for every full-history rebuild",
    )
    parser.add_argument(
        "--source-policy",
        type=Path,
        default=Path("source_replay_policy.json"),
        help="Versioned source-level warehouse replay exclusion policy",
    )
    parser.add_argument("--config", type=Path, default=Path("open_data_config.yml"))
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path("data/recovery/snapshot-replay/archives"),
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        default=Path("data/recovery/snapshot-replay/staging"),
    )
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument(
        "--generation-id",
        help="Optional rebuild generation ID; GitHub run ID and attempt are used by default.",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--database",
        type=Path,
        help="Explicit isolated DuckDB candidate path for rebuild mode",
    )
    parser.add_argument("--dataset", default="raw")
    parser.add_argument(
        "--schema",
        help="Explicit run-scoped Snowflake candidate schema for rebuild mode",
    )
    parser.add_argument(
        "--from-date",
        help=(
            "Inclusive UTC extraction date lower bound (rebuild mode only; YYYY-MM-DD)"
        ),
    )
    parser.add_argument(
        "--to-date",
        help=(
            "Inclusive UTC extraction date upper bound (rebuild mode only; YYYY-MM-DD)"
        ),
    )
    parser.add_argument(
        "--limit",
        "--last-n",
        dest="limit",
        type=_positive_int,
        help=(
            "select the most recent N archives within the selected rebuild scope, "
            "then replay them chronologically; marks the result partial"
        ),
    )
    parser.add_argument(
        "--snowflake-credentials",
        default=os.getenv("SNOWFLAKE_DLT_CONNECTION"),
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    generation_id = None
    generation_started = False

    try:
        if args.mode == "rebuild":
            generation_id = _generation_id(args)
            args.generation_id = generation_id

        report = run(args)
        if args.mode == "rebuild":
            generation_started = True

            # This validates one backend only. The optional cross-backend
            # comparison job does not control this backend's generation status.
            from scripts.validate_snapshot_replay import validate_backend_report

            validate_backend_report(
                report,
                args.backend,
                source_exclusions=load_source_replay_policy(
                    Path(
                        getattr(
                            args,
                            "source_policy",
                            Path("source_replay_policy.json"),
                        )
                    )
                ),
            )
            ReplayLedger(args.ledger).finish_generation(
                args.backend, generation_id, "SUCCEEDED"
            )
    except Exception as exc:
        if generation_started and generation_id is not None:
            try:
                ReplayLedger(args.ledger).finish_generation(
                    args.backend,
                    generation_id,
                    "FAILED",
                    error=f"{type(exc).__name__}: {exc}",
                )
            except Exception as ledger_error:
                print(
                    f"Could not persist generation failure: {ledger_error}",
                    file=sys.stderr,
                )
        print(f"Replay failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
