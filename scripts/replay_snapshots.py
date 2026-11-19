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

from scripts.replay_archive import SnapshotArchive, inspect_archive
from scripts.replay_ledger import ReplayLedger
from scripts.replay_targets import (
    DEFAULT_DUCKDB_ROOT,
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


def _validate_full_history_inventory(args: argparse.Namespace) -> list[SnapshotArchive]:
    """Verify the complete Release set and every archive before opening a candidate."""
    inventory = load_inventory(args.inventory, args.repository)
    assets = list_release_assets(args.repository)
    validate_inventory_assets(inventory, assets)
    archives = discover_archives(
        args.repository,
        args.archive_dir,
        exclude_snapshot_ids=set(),
    )
    validate_inventory_archives(inventory, archives, args.repository)
    return archives


def run(args: argparse.Namespace) -> dict[str, Any]:
    # Validate scope before loading config, creating the local ledger, connecting
    # to Snowflake, or opening a DuckDB candidate.
    _validate_selection_request(args)
    # Mode 2 verifies the complete source history before creating or opening a
    # DuckDB/Snowflake candidate. Incremental Mode 1 keeps its production-ledger path.
    prevalidated_rebuild_archives = (
        _validate_full_history_inventory(args) if args.mode == "rebuild" else None
    )
    source_configs = _source_configs(args.config)
    ledger = ReplayLedger(args.ledger)

    # Mode 1 opens its target first to skip already-promoted archive IDs.
    # Full-history Mode 2 has already verified and downloaded every archive above.
    target = _build_target(args)
    target_completed = target.completed_snapshot_ids()
    run_completed = ledger.completed_snapshot_ids(args.backend)

    if args.mode == "incremental":
        archives_discovered = discover_archives(
            args.repository,
            args.archive_dir,
            exclude_snapshot_ids=target_completed | run_completed,
        )
        selected_archives, history_scope = select_archives(
            archives_discovered,
            mode=args.mode,
        )
    else:
        # For rebuilds, discover the full archive sequence first. This keeps
        # --limit N stable across retries: completed items are skipped by the
        # persisted ledgers only after the date range and N-snapshot scope is fixed.
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

    pending_before_load = sum(
        archive.snapshot_id not in (target_completed | run_completed)
        for archive in selected_archives
    )
    report = replay_backend(
        selected_archives,
        source_configs=source_configs,
        target=target,
        ledger_path=args.ledger,
        staging_root=args.staging_dir,
    )
    report.update(
        {
            "mode": args.mode,
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
            "ledger_entries": ledger.snapshot_records(args.backend),
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
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
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DUCKDB_ROOT / "toronto-replay.duckdb",
    )
    parser.add_argument("--dataset", default="raw")
    parser.add_argument("--schema", default="TORONTO_SNAPSHOT_REPLAY_0")
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
    try:
        report = run(args)
    except Exception as exc:
        print(f"Replay failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
