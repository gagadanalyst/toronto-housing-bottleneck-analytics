"""Publish rolling release assets with a verified rollback path."""

from __future__ import annotations

import filecmp
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Protocol

RELEASE_TAG = "warehouse-latest"
RELEASE_TITLE = "Latest Toronto Housing Data Assets"
RELEASE_NOTES = """Rolling data publication, not a semantic-version release.

Assets:
- toronto-active-permits.parquet: full-source snapshot for Active Permits.
- toronto-cleared-permits.parquet: full-source snapshot for Cleared Permits.
- toronto.duckdb: source warehouse tables plus dbt staging views and mart tables.
- toronto-dbt-docs.zip: complete static dbt documentation site, including assets and generated index files, plus viewing instructions. Extract it and run `python3 -m http.server 8000` in the extracted directory, then open http://localhost:8000/.
"""
ASSET_PATHS = {
    "toronto-active-permits.parquet": Path(
        "data-assets/toronto-active-permits.parquet"
    ),
    "toronto-cleared-permits.parquet": Path(
        "data-assets/toronto-cleared-permits.parquet"
    ),
    "toronto.duckdb": Path("data/warehouse/toronto.duckdb"),
    "toronto-dbt-docs.zip": Path("data-assets/toronto-dbt-docs.zip"),
}
ASSET_NAMES = tuple(ASSET_PATHS)


class ReleaseClient(Protocol):
    """Minimal release operations required by the publisher."""

    def release_state(self, tag: str) -> str: ...
    def download(self, tag: str, directory: Path) -> None: ...
    def create_release(
        self, tag: str, paths: list[Path], title: str, notes: str, target: str
    ) -> None: ...
    def upload(self, tag: str, path: Path) -> None: ...
    def edit(self, tag: str, title: str, notes: str) -> None: ...
    def delete(self, tag: str) -> None: ...


class GitHubReleaseClient:
    """Use the authenticated GitHub CLI to manage release assets."""

    def release_state(self, tag: str) -> str:
        result = subprocess.run(
            ["gh", "release", "view", tag, "--json", "isDraft", "--jq", ".isDraft"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode:
            return "missing"
        return "draft" if result.stdout.strip() == "true" else "published"

    def download(self, tag: str, directory: Path) -> None:
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        subprocess.run(
            ["gh", "release", "download", tag, "--dir", str(directory), "--clobber"],
            check=True,
        )

    def create_release(
        self, tag: str, paths: list[Path], title: str, notes: str, target: str
    ) -> None:
        subprocess.run(
            [
                "gh",
                "release",
                "create",
                tag,
                *(str(path) for path in paths),
                "--draft",
                "--title",
                title,
                "--notes",
                notes,
                "--latest=false",
                "--target",
                target,
            ],
            check=True,
        )

    def upload(self, tag: str, path: Path) -> None:
        subprocess.run(
            ["gh", "release", "upload", tag, str(path), "--clobber"], check=True
        )

    def edit(self, tag: str, title: str, notes: str) -> None:
        # Without --draft, editing a draft release publishes it.
        subprocess.run(
            [
                "gh",
                "release",
                "edit",
                tag,
                "--title",
                title,
                "--notes",
                notes,
                "--latest=false",
                "--draft=false",
            ],
            check=True,
        )

    def delete(self, tag: str) -> None:
        subprocess.run(["gh", "release", "delete", tag, "--yes"], check=True)


def _validate_asset_directory(directory: Path) -> None:
    for name in ASSET_NAMES:
        path = directory / name
        if not path.is_file() or path.stat().st_size == 0:
            raise RuntimeError(f"Missing or empty release asset: {path}")


def _verify_published(
    client: ReleaseClient, expected_paths: dict[str, Path], directory: Path
) -> None:
    client.download(RELEASE_TAG, directory)
    _validate_asset_directory(directory)
    for name in ASSET_NAMES:
        if not filecmp.cmp(expected_paths[name], directory / name, shallow=False):
            raise RuntimeError(f"Published asset integrity check failed: {name}")


def _upload_with_retry(client: ReleaseClient, path: Path) -> None:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            client.upload(RELEASE_TAG, path)
            return
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(attempt + 1)
    assert last_error is not None
    raise last_error


def _restore_last_known_good(
    client: ReleaseClient, backup_dir: Path, recovery_root: Path
) -> None:
    failures = []
    for name in ASSET_NAMES:
        try:
            _upload_with_retry(client, backup_dir / name)
        except Exception as exc:
            failures.append(f"{name}: {exc}")
    if failures:
        raise RuntimeError("Rollback upload errors: " + "; ".join(failures))
    old_paths = {name: backup_dir / name for name in ASSET_NAMES}
    _verify_published(client, old_paths, recovery_root / "rollback-check")


def publish_release_assets(
    client: ReleaseClient,
    asset_paths: dict[str, Path],
    recovery_root: Path,
    *,
    simulate_failure_after_upload: int | None = None,
) -> None:
    """Publish assets, rolling back any failed replacement of an existing release."""
    if set(asset_paths) != set(ASSET_NAMES):
        raise ValueError(f"Expected exactly these assets: {ASSET_NAMES}")
    for name in ASSET_NAMES:
        path = asset_paths[name]
        if not path.is_file() or path.stat().st_size == 0:
            raise RuntimeError(f"Missing or empty candidate asset: {path}")

    shutil.rmtree(recovery_root, ignore_errors=True)
    state = client.release_state(RELEASE_TAG)
    if state == "draft":
        # Clean up a draft left behind by an interrupted first publication.
        client.delete(RELEASE_TAG)
        state = "missing"

    if state != "published":
        # First publication: stage the complete release as a draft before exposing it.
        try:
            client.create_release(
                RELEASE_TAG,
                [asset_paths[name] for name in ASSET_NAMES],
                RELEASE_TITLE,
                RELEASE_NOTES,
                os.environ.get("WAREHOUSE_LATEST_TARGET_SHA", "main"),
            )
            _verify_published(client, asset_paths, recovery_root / "draft-check")
            client.edit(RELEASE_TAG, RELEASE_TITLE, RELEASE_NOTES)
            _verify_published(client, asset_paths, recovery_root / "published-check")
        except Exception as create_error:
            try:
                if client.release_state(RELEASE_TAG) != "missing":
                    client.delete(RELEASE_TAG)
            except Exception as cleanup_error:
                raise RuntimeError(
                    f"Initial publication failed ({create_error}); cleanup also failed "
                    f"({cleanup_error})."
                ) from create_error
            raise RuntimeError(
                f"Initial release creation/verification failed: {create_error}"
            ) from create_error
        finally:
            shutil.rmtree(recovery_root, ignore_errors=True)
        return

    backup_dir = recovery_root / "last-known-good"
    try:
        client.download(RELEASE_TAG, backup_dir)
        _validate_asset_directory(backup_dir)
    except Exception as exc:
        shutil.rmtree(recovery_root, ignore_errors=True)
        raise RuntimeError(
            "Could not back up all four existing assets; publication was not attempted."
        ) from exc

    try:
        for index, name in enumerate(ASSET_NAMES, start=1):
            client.upload(RELEASE_TAG, asset_paths[name])
            if simulate_failure_after_upload == index:
                raise RuntimeError(f"Simulated failure after upload {index}")
        _verify_published(client, asset_paths, recovery_root / "published-check")
        client.edit(RELEASE_TAG, RELEASE_TITLE, RELEASE_NOTES)
    except Exception as publication_error:
        try:
            _restore_last_known_good(client, backup_dir, recovery_root)
        except Exception as rollback_error:
            raise RuntimeError(
                f"Publication failed ({publication_error}); rollback also failed ({rollback_error}). "
                f"Recovery backup retained at {backup_dir}."
            ) from publication_error
        shutil.rmtree(recovery_root, ignore_errors=True)
        raise RuntimeError(
            f"Publication failed ({publication_error}); last-known-good assets were restored and verified."
        ) from publication_error
    shutil.rmtree(recovery_root, ignore_errors=True)


def main() -> int:
    recovery_dir = os.environ.get("WAREHOUSE_LATEST_BACKUP_DIR")
    if not recovery_dir:
        print("WAREHOUSE_LATEST_BACKUP_DIR must be set.", file=sys.stderr)
        return 2
    try:
        publish_release_assets(GitHubReleaseClient(), ASSET_PATHS, Path(recovery_dir))
    except Exception as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1
    print("All four release assets were published and verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
