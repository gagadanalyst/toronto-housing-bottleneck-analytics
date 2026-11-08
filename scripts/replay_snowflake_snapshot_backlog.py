"""Replay approved monthly raw snapshot archives into Snowflake chronologically."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    from snapshot_archive_backlog import discover_archives
except ImportError:
    from scripts.snapshot_archive_backlog import discover_archives


def _pending_archives(archives, completed_snapshot_ids: set[str]):
    """Keep only archive identities not already complete in the Snowflake ledger."""
    return [item for item in archives if item.snapshot_id not in completed_snapshot_ids]


def _archives_through_selection(archives, *, run_id: str, attempt: str):
    """Return the chronological backlog through the requested manual archive."""
    selected_index = next(
        (
            index
            for index, item in enumerate(archives)
            if item.run_id == run_id and item.attempt == attempt
        ),
        None,
    )
    if selected_index is None:
        raise ValueError(
            f"Selected archive {run_id}:{attempt} is missing or not approved "
            "in the monthly Release backlog."
        )
    return archives[: selected_index + 1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id")
    parser.add_argument("--attempt")
    parser.add_argument("--release-tag")
    parser.add_argument("--asset-name")
    args = parser.parse_args()

    selected_values = (args.run_id, args.attempt, args.release_tag, args.asset_name)
    manual = any(value is not None for value in selected_values)
    if manual and not all(value for value in selected_values):
        parser.error(
            "Manual replay requires --run-id, --attempt, --release-tag and --asset-name."
        )

    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if not repository:
        raise ValueError(
            "GITHUB_REPOSITORY is required for snapshot backlog discovery."
        )
    if not os.environ.get("TORONTO_WAREHOUSE_CREDENTIALS"):
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is missing.")

    if manual:
        expected_asset = f"toronto-raw-snapshot-bundle-{args.run_id}-{args.attempt}.zip"
        if args.asset_name != expected_asset:
            raise ValueError(
                "Manual archive asset does not match the selected run ID and attempt."
            )

    with tempfile.TemporaryDirectory(
        prefix="toronto-snowflake-snapshot-backlog-"
    ) as temp:
        # Read the durable ledger before downloading assets. Fully completed
        # archives remain in Releases but need not be downloaded on every sweep.
        try:
            from snowflake_refresh_raw import (
                _assert_canonical_replay_baseline,
                _completed_snapshot_ids,
                _snowflake_connection_options,
            )
        except ImportError:
            from scripts.snowflake_refresh_raw import (
                _assert_canonical_replay_baseline,
                _completed_snapshot_ids,
                _snowflake_connection_options,
            )
        credentials = os.environ["TORONTO_WAREHOUSE_CREDENTIALS"]
        key_path = Path(temp) / "snowflake_backlog_key.p8"
        connection_options = _snowflake_connection_options(credentials, key_path)
        _assert_canonical_replay_baseline(connection_options)
        completed_ids = _completed_snapshot_ids(connection_options)
        include_ids = {f"{args.run_id}:{args.attempt}"} if manual else set()

        archives = discover_archives(
            repository,
            Path(temp),
            exclude_snapshot_ids=completed_ids,
            include_snapshot_ids=include_ids,
        )
        if manual:
            archives = _archives_through_selection(
                archives,
                run_id=args.run_id,
                attempt=args.attempt,
            )
            selected = archives[-1]
            if (selected.release_tag, selected.asset_name) != (
                args.release_tag,
                args.asset_name,
            ):
                raise ValueError(
                    "Manual Release tag/asset does not match the selected archived snapshot."
                )
        else:
            dispatch_run_id = os.environ.get("SNAPSHOT_SOURCE_RUN_ID", "")
            dispatch_attempt = os.environ.get("SNAPSHOT_SOURCE_RUN_ATTEMPT", "")
            if (
                dispatch_run_id
                and f"{dispatch_run_id}:{dispatch_attempt}" not in completed_ids
            ):
                dispatched = [
                    item
                    for item in archives
                    if item.run_id == dispatch_run_id
                    and item.attempt == dispatch_attempt
                ]
                if len(dispatched) != 1:
                    raise ValueError(
                        "The repository_dispatch snapshot is not present exactly once "
                        "in the approved archive backlog."
                    )
                dispatch_tag = os.environ.get("SNAPSHOT_ARCHIVE_TAG", "")
                dispatch_asset = os.environ.get("SNAPSHOT_ARCHIVE_ASSET", "")
                dispatch_commit = os.environ.get("SNAPSHOT_SOURCE_COMMIT", "")
                selected = dispatched[0]
                if (selected.release_tag, selected.asset_name) != (
                    dispatch_tag,
                    dispatch_asset,
                ):
                    raise ValueError(
                        "repository_dispatch Release tag/asset identity does not match the archived snapshot."
                    )
                if (
                    dispatch_commit
                    and selected.bundle.get("source_commit") != dispatch_commit
                ):
                    raise ValueError(
                        "repository_dispatch source commit does not match the archived snapshot."
                    )

        if not archives:
            print("No approved raw snapshot archives are waiting for Snowflake replay.")
            return 0

        pending = _pending_archives(archives, completed_ids)
        if not pending:
            print("All approved Snowflake archives are already fully loaded.")
            return 0

        for item in pending:
            env = os.environ.copy()
            env.update(
                {
                    "SNAPSHOT_SOURCE_RUN_ID": item.run_id,
                    "SNAPSHOT_SOURCE_RUN_ATTEMPT": item.attempt,
                    "SNAPSHOT_SOURCE_COMMIT": str(item.bundle["source_commit"]),
                    "SNAPSHOT_ARCHIVE_TAG": item.release_tag,
                    "SNAPSHOT_ARCHIVE_ASSET": item.asset_name,
                    "SNAPSHOT_BUNDLE_PATH": str(item.path),
                }
            )
            print(
                f"Replaying Snowflake archive {item.snapshot_id} "
                f"({item.extracted_at.isoformat()}) from {item.release_tag}/{item.asset_name}."
            )
            subprocess.run(
                [sys.executable, "scripts/snowflake_refresh_raw.py"],
                check=True,
                env=env,
            )

    print("Snowflake approved snapshot backlog replay completed in extraction order.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
