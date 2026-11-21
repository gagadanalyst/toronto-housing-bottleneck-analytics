"""Idempotently seed loading-latest from the existing verified warehouse."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from scripts.publish_loading_assets import GitHubReleaseClient, publish_assets
from scripts.restore_duckdb_production import (
    _validate_database,
    restore_production_database,
)


def _download_asset(repository: str, tag: str, name: str, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
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
            str(directory),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    path = directory / name
    if not path.is_file() or path.stat().st_size <= 0:
        raise RuntimeError(f"Release {tag} is missing required asset {name}.")
    return path


def seed_loading_release(repository: str, backup_dir: Path) -> str:
    client = GitHubReleaseClient(repository)
    state = client.state("loading-latest")
    if state == "published":
        with tempfile.TemporaryDirectory(prefix="toronto-loading-verify-") as temp:
            root = Path(temp)
            client.download("loading-latest", root)
            database = root / "toronto-loading.duckdb"
            for name in (
                "toronto-active-permits.parquet",
                "toronto-cleared-permits.parquet",
            ):
                path = root / name
                if not path.is_file() or path.stat().st_size <= 0:
                    raise RuntimeError(
                        f"loading-latest is incomplete: {name} is missing."
                    )
            _validate_database(database)
        print("loading-latest already exists and passed baseline validation.")
        return "existing"
    if state == "draft":
        raise RuntimeError(
            "loading-latest is an interrupted draft; inspect it before seeding."
        )

    with tempfile.TemporaryDirectory(prefix="toronto-loading-seed-") as temp:
        root = Path(temp)
        warehouse_database = root / "toronto.duckdb"
        # Preserve the established immutable recovery fallback for the initial seed only.
        restore_production_database(repository, warehouse_database)
        _validate_database(warehouse_database)
        active = _download_asset(
            repository,
            "warehouse-latest",
            "toronto-active-permits.parquet",
            root / "assets",
        )
        cleared = _download_asset(
            repository,
            "warehouse-latest",
            "toronto-cleared-permits.parquet",
            root / "assets",
        )
        loading_database = root / "toronto-loading.duckdb"
        shutil.copy2(warehouse_database, loading_database)
        publish_assets(
            client,
            {
                "toronto-loading.duckdb": loading_database,
                "toronto-active-permits.parquet": active,
                "toronto-cleared-permits.parquet": cleared,
            },
            backup_dir,
            target=os.environ.get("GITHUB_SHA", "main"),
        )
    print("Seeded loading-latest from the verified existing warehouse baseline.")
    return "seeded"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument(
        "--backup-dir", type=Path, default=Path(".tmp/loading-seed-backup")
    )
    args = parser.parse_args()
    if not args.repository:
        parser.error("--repository or GITHUB_REPOSITORY is required")
    seed_loading_release(args.repository, args.backup_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
