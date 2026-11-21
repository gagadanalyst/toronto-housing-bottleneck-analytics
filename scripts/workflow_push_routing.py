"""Fail-closed push routing for production GitHub Actions workflows."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")

OUTPUT_NAMES = {
    "loading": "should_load",
    "transformation": "should_transform",
}

LOADING_EXTRACTION_PATHS = (
    "extraction/",
    ".github/workflows/v2-snapshot-archive.yml",
    "scripts/snapshot_inventory.py",
)

LOADING_RUNTIME_PATHS = (
    "pyproject.toml",
    "src/toronto_housing_ingestion/",
    "scripts/mode1_state.py",
    "scripts/replay_",
    "scripts/promote_duckdb_candidate.py",
    "scripts/loader_promote_snowflake_raw.py",
    "scripts/snapshot_replay_baseline.py",
    "scripts/prepare_duckdb_candidate.py",
    "scripts/extract_latest_snapshot_assets.py",
    "scripts/snowflake_candidate_utils.py",
    "scripts/restore_duckdb_production.py",
    "scripts/publish_loading_assets.py",
    "scripts/seed_loading_release.py",
)

TRANSFORMATION_UPSTREAM_PATHS = (
    "extraction/",
    ".github/workflows/v2-snapshot-archive.yml",
    ".github/workflows/mode1-incremental-loader.yml",
    ".github/workflows/promote-snapshot-rebuild.yml",
    ".github/workflows/snapshot-replay.yml",
    "pyproject.toml",
    "src/toronto_housing_ingestion/",
    "scripts/mode1_state.py",
    "scripts/replay_",
    "scripts/loader_promote_snowflake_raw.py",
    "scripts/prepare_duckdb_candidate.py",
    "scripts/promote_duckdb_candidate.py",
    "scripts/snowflake_candidate_utils.py",
    "scripts/promote_duckdb_snapshot_rebuild.py",
    "scripts/promote_snowflake_snapshot_rebuild.py",
    "scripts/validate_full_rebuild_promotion.py",
    "scripts/snapshot_replay_baseline.py",
    "scripts/restore_duckdb_production.py",
    "scripts/publish_loading_assets.py",
    "scripts/seed_loading_release.py",
    "scripts/extract_latest_snapshot_assets.py",
)

TRANSFORMATION_RUNTIME_PATHS = (
    ".github/workflows/snowflake-serving-schema.yml",
    "dbt/",
    "dbt_project.yml",
    "scripts/publish_release_assets.py",
    "scripts/publish_duckdb_recovery.py",
    "scripts/prepare_snowflake_serving_candidate.py",
    "scripts/promote_snowflake_serving_candidate.py",
)


def _matches(path: str, patterns: tuple[str, ...]) -> bool:
    for pattern in patterns:
        if pattern.endswith(("/", "_")):
            if path.startswith(pattern):
                return True
        elif path == pattern:
            return True
    return False


def get_changed_paths(
    repository: Path,
    before_sha: str,
    after_sha: str,
) -> tuple[str, ...]:
    """Read changed paths from the actual Git commit range."""

    for label, sha in (("before", before_sha), ("after", after_sha)):
        if not SHA_RE.fullmatch(sha) or sha == "0" * 40:
            raise ValueError(f"Invalid or unavailable {label} commit SHA")

        subprocess.run(
            [
                "git",
                "-C",
                str(repository),
                "cat-file",
                "-e",
                f"{sha}^{{commit}}",
            ],
            check=True,
            capture_output=True,
        )

    result = subprocess.run(
        [
            "git",
            "-C",
            str(repository),
            "diff",
            "--name-only",
            "--no-renames",
            "-z",
            before_sha,
            after_sha,
        ],
        check=True,
        capture_output=True,
    )

    return tuple(os.fsdecode(item) for item in result.stdout.split(b"\0") if item)


def should_run_push(mode: str, changed_paths: tuple[str, ...]) -> bool:
    """Return whether this push warrants a production stage run."""

    if not changed_paths:
        return False

    if mode == "loading":
        if _matches_any(changed_paths, LOADING_EXTRACTION_PATHS):
            return False
        return _matches_any(changed_paths, LOADING_RUNTIME_PATHS)

    if mode == "transformation":
        if _matches_any(changed_paths, TRANSFORMATION_UPSTREAM_PATHS):
            return False
        return _matches_any(changed_paths, TRANSFORMATION_RUNTIME_PATHS)

    raise ValueError(f"Unsupported routing mode: {mode}")


def _matches_any(
    paths: tuple[str, ...],
    patterns: tuple[str, ...],
) -> bool:
    return any(_matches(path, patterns) for path in paths)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("loading", "transformation"), required=True)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", required=True)
    args = parser.parse_args()

    try:
        changed = get_changed_paths(args.repo, args.before, args.after)
        should_run = should_run_push(args.mode, changed)
        print(
            f"Push routing inspected {len(changed)} changed file(s).",
            file=sys.stderr,
        )
        for changed_path in changed:
            print(f"  {changed_path}", file=sys.stderr)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f"Push routing failed; skipping production stage: {exc}", file=sys.stderr)
        should_run = False

    print(f"{OUTPUT_NAMES[args.mode]}={str(should_run).lower()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
