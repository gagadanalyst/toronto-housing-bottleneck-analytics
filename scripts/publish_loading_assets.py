"""Publish the independent DuckDB SCD2 loading baseline safely."""

from __future__ import annotations

import argparse
import filecmp
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Protocol

RELEASE_TAG = "loading-latest"
TITLE = "Latest Toronto Housing Loading Baseline"
NOTES = """Authoritative DuckDB SCD2 loading baseline, separate from dashboard serving.

Assets:
- toronto-loading.duckdb: RAW/SCD2 tables, snapshot ledger and DLT state.
- toronto-active-permits.parquet: latest approved full-source Active snapshot.
- toronto-cleared-permits.parquet: latest approved full-source Cleared snapshot.

The dashboard continues to consume warehouse-latest, published by Transformation.
"""
ASSETS = {
    "toronto-loading.duckdb": Path("data/warehouse/toronto-loading.duckdb"),
    "toronto-active-permits.parquet": Path(
        "data-assets/toronto-active-permits.parquet"
    ),
    "toronto-cleared-permits.parquet": Path(
        "data-assets/toronto-cleared-permits.parquet"
    ),
}


class ReleaseClient(Protocol):
    def state(self, tag: str) -> str: ...
    def download(self, tag: str, directory: Path) -> None: ...
    def create(self, tag: str, paths: list[Path], target: str) -> None: ...
    def upload(self, tag: str, path: Path) -> None: ...
    def edit(self, tag: str) -> None: ...
    def delete(self, tag: str) -> None: ...


class GitHubReleaseClient:
    def __init__(self, repository: str):
        self.repository = repository

    def run(self, *args: str, check: bool = True):
        result = subprocess.run(
            ["gh", *args, "--repo", self.repository],
            check=False,
            capture_output=True,
            text=True,
        )
        if check and result.returncode:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())
        return result

    def state(self, tag: str) -> str:
        result = self.run("release", "view", tag, "--json", "isDraft", check=False)
        if result.returncode:
            message = f"{result.stdout}\n{result.stderr}".lower()
            if "404" in message or "not found" in message:
                return "missing"
            raise RuntimeError(f"Could not inspect {tag}: {result.stderr.strip()}")
        return "draft" if json.loads(result.stdout).get("isDraft") else "published"

    def download(self, tag: str, directory: Path) -> None:
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True, exist_ok=True)
        self.run("release", "download", tag, "--dir", str(directory))

    def create(self, tag: str, paths: list[Path], target: str) -> None:
        self.run(
            "release",
            "create",
            tag,
            *(str(p) for p in paths),
            "--draft",
            "--title",
            TITLE,
            "--notes",
            NOTES,
            "--latest=false",
            "--target",
            target,
        )

    def upload(self, tag: str, path: Path) -> None:
        self.run("release", "upload", tag, str(path), "--clobber")

    def edit(self, tag: str) -> None:
        self.run(
            "release",
            "edit",
            tag,
            "--title",
            TITLE,
            "--notes",
            NOTES,
            "--latest=false",
            "--draft=false",
        )

    def delete(self, tag: str) -> None:
        self.run("release", "delete", tag, "--yes")


def _validate_assets(paths: dict[str, Path]) -> None:
    if set(paths) != set(ASSETS):
        raise ValueError(f"Expected exactly these assets: {tuple(ASSETS)}")
    for name, path in paths.items():
        if not path.is_file() or path.stat().st_size <= 0:
            raise RuntimeError(f"Missing or empty loading asset {name}: {path}")


def _verify(client: ReleaseClient, paths: dict[str, Path], directory: Path) -> None:
    client.download(RELEASE_TAG, directory)
    for name, expected in paths.items():
        observed = directory / name
        if not observed.is_file() or not filecmp.cmp(expected, observed, shallow=False):
            raise RuntimeError(f"loading-latest integrity check failed: {name}")


def _upload_retry(client: ReleaseClient, tag: str, path: Path) -> None:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            client.upload(tag, path)
            return
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(attempt + 1)
    assert last_error is not None
    raise last_error


def publish_assets(
    client: ReleaseClient,
    paths: dict[str, Path],
    backup_dir: Path,
    *,
    target: str = "main",
    simulate_failure_after_upload: int | None = None,
) -> None:
    """Publish all three loading assets and verify or roll back the release."""
    _validate_assets(paths)
    shutil.rmtree(backup_dir, ignore_errors=True)
    state = client.state(RELEASE_TAG)
    if state == "draft":
        client.delete(RELEASE_TAG)
        state = "missing"

    if state == "missing":
        try:
            client.create(RELEASE_TAG, [paths[name] for name in ASSETS], target)
            _verify(client, paths, backup_dir / "draft-check")
            client.edit(RELEASE_TAG)
            _verify(client, paths, backup_dir / "published-check")
        except Exception as create_error:
            try:
                if client.state(RELEASE_TAG) != "missing":
                    client.delete(RELEASE_TAG)
            except Exception as cleanup_error:
                raise RuntimeError(
                    f"Initial loading publication failed ({create_error}); cleanup failed ({cleanup_error})."
                ) from create_error
            raise RuntimeError(
                f"Initial loading release creation failed: {create_error}"
            ) from create_error
        finally:
            shutil.rmtree(backup_dir, ignore_errors=True)
        return

    backup = backup_dir / "last-known-good"
    client.download(RELEASE_TAG, backup)
    for name in ASSETS:
        old = backup / name
        if not old.is_file() or old.stat().st_size <= 0:
            raise RuntimeError(
                f"Cannot update loading-latest: prior asset missing: {name}"
            )

    try:
        for index, name in enumerate(ASSETS, start=1):
            _upload_retry(client, RELEASE_TAG, paths[name])
            if simulate_failure_after_upload == index:
                raise RuntimeError(f"Simulated failure after upload {index}")
        _verify(client, paths, backup_dir / "published-check")
        client.edit(RELEASE_TAG)
    except Exception as publication_error:
        rollback_errors: list[str] = []
        for name in ASSETS:
            try:
                _upload_retry(client, RELEASE_TAG, backup / name)
            except Exception as exc:
                rollback_errors.append(f"{name}: {exc}")
        if rollback_errors:
            raise RuntimeError(
                f"Loading publication failed ({publication_error}); rollback failed "
                f"({'; '.join(rollback_errors)}). Backup retained at {backup}."
            ) from publication_error
        try:
            _verify(
                client,
                {name: backup / name for name in ASSETS},
                backup_dir / "rollback-check",
            )
        except Exception as exc:
            raise RuntimeError(
                f"Loading publication failed ({publication_error}); rollback verification failed ({exc}). "
                f"Backup retained at {backup}."
            ) from publication_error
        raise RuntimeError(
            f"Loading publication failed ({publication_error}); previous loading assets were restored and verified."
        ) from publication_error
    shutil.rmtree(backup_dir, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument(
        "--database", type=Path, default=ASSETS["toronto-loading.duckdb"]
    )
    parser.add_argument(
        "--active-parquet", type=Path, default=ASSETS["toronto-active-permits.parquet"]
    )
    parser.add_argument(
        "--cleared-parquet",
        type=Path,
        default=ASSETS["toronto-cleared-permits.parquet"],
    )
    parser.add_argument(
        "--backup-dir", type=Path, default=Path(".tmp/loading-latest-backup")
    )
    args = parser.parse_args()
    if not args.repository:
        parser.error("--repository or GITHUB_REPOSITORY is required")
    try:
        publish_assets(
            GitHubReleaseClient(args.repository),
            {
                "toronto-loading.duckdb": args.database,
                "toronto-active-permits.parquet": args.active_parquet,
                "toronto-cleared-permits.parquet": args.cleared_parquet,
            },
            args.backup_dir,
            target=os.environ.get("GITHUB_SHA", "main"),
        )
    except Exception as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1
    print("All three loading-latest assets were published and verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
