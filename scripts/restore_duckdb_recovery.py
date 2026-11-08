"""Restore the latest verified immutable monthly DuckDB recovery checkpoint."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

RELEASE_RE = re.compile(r"toronto-duckdb-recovery-(\d{4})-(\d{2})\Z")
DB_ASSET_RE = re.compile(r"toronto-duckdb-recovery-(\d+)-(\d+)\.duckdb\Z")


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["gh", *args], check=True, capture_output=True, text=True)


def _flatten(value: Any) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    if isinstance(value, list):
        for item in value:
            output.extend(_flatten(item))
    elif isinstance(value, dict):
        output.append(value)
    return output


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _available_checkpoints(repository: str) -> list[tuple[datetime, str, str, str]]:
    pages = json.loads(
        _run(
            "api", "--paginate", "--slurp", f"repos/{repository}/releases?per_page=100"
        ).stdout
        or "[]"
    )
    releases = _flatten(pages)
    results: list[tuple[datetime, str, str, str]] = []
    for release in releases:
        tag = str(release.get("tag_name", ""))
        if (
            not RELEASE_RE.fullmatch(tag)
            or release.get("draft")
            or release.get("prerelease")
        ):
            continue
        release_id = int(release["id"])
        pages = json.loads(
            _run(
                "api",
                "--paginate",
                "--slurp",
                f"repos/{repository}/releases/{release_id}/assets?per_page=100",
            ).stdout
            or "[]"
        )
        assets = _flatten(pages)
        asset_names = [str(asset.get("name")) for asset in assets]
        duplicates = sorted(
            {name for name in asset_names if asset_names.count(name) > 1}
        )
        if duplicates:
            raise RuntimeError(
                f"Recovery Release {tag} has duplicate asset names: {duplicates}"
            )
        by_name = {str(asset.get("name")): asset for asset in assets}
        for asset_name in by_name:
            match = DB_ASSET_RE.fullmatch(asset_name)
            if not match:
                continue
            prefix = asset_name[: -len(".duckdb")]
            manifest_name = prefix + ".json"
            db_asset = by_name[asset_name]
            manifest_asset = by_name.get(manifest_name)
            if manifest_asset is None:
                continue
            created = datetime.fromisoformat(
                str(db_asset["created_at"]).replace("Z", "+00:00")
            ).astimezone(UTC)
            results.append((created, tag, asset_name, manifest_name))
    results.sort(key=lambda item: item[0], reverse=True)
    return results


def restore_latest(repository: str, destination: Path) -> bool:
    checkpoints = _available_checkpoints(repository)
    if not checkpoints:
        return False

    created_at, tag, db_name, metadata_name = checkpoints[0]
    with tempfile.TemporaryDirectory(prefix="toronto-duckdb-recovery-restore-") as temp:
        root = Path(temp)
        _run(
            "release",
            "download",
            tag,
            "--repo",
            repository,
            "--pattern",
            db_name,
            "--dir",
            temp,
        )
        _run(
            "release",
            "download",
            tag,
            "--repo",
            repository,
            "--pattern",
            metadata_name,
            "--dir",
            temp,
        )
        db_path = root / db_name
        metadata_path = root / metadata_name
        if not db_path.is_file() or not metadata_path.is_file():
            raise RuntimeError(
                "Latest recovery Release is missing its paired database or metadata asset."
            )
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        match = DB_ASSET_RE.fullmatch(db_name)
        if not match:
            raise ValueError("Recovery database asset name is invalid.")
        if (
            metadata.get("format_version") != 1
            or metadata.get("database_asset") != db_name
            or metadata.get("release_tag") != tag
            or str(metadata.get("checkpoint_run_id")) != match.group(1)
            or str(metadata.get("checkpoint_run_attempt")) != match.group(2)
            or int(metadata.get("database_size_bytes", -1)) != db_path.stat().st_size
            or metadata.get("database_sha256") != _sha256(db_path)
        ):
            raise RuntimeError(
                "Latest recovery database does not match its immutable metadata."
            )

        # Verify it opens as DuckDB and contains the durable per-source replay ledger.
        with duckdb.connect(str(db_path), read_only=True) as connection:
            tables = {
                str(row[0]).lower()
                for row in connection.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema='raw'"
                ).fetchall()
            }
            if "toronto_snapshot_ledger" not in tables:
                raise RuntimeError(
                    "Recovery database is missing raw.toronto_snapshot_ledger."
                )

        destination.parent.mkdir(parents=True, exist_ok=True)
        staged = destination.with_name(destination.name + ".restore.tmp")
        shutil.copy2(db_path, staged)
        if _sha256(staged) != metadata["database_sha256"]:
            staged.unlink(missing_ok=True)
            raise RuntimeError(
                "Staged DuckDB recovery copy failed checksum verification."
            )
        staged.replace(destination)

    print(
        f"Restored verified immutable DuckDB checkpoint {tag}/{db_name} uploaded {created_at.isoformat()}."
    )
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=Path("data/warehouse/toronto.duckdb")
    )
    args = parser.parse_args()
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if (
        not repository
        or not os.environ.get("GH_TOKEN")
        and not os.environ.get("GITHUB_TOKEN")
    ):
        raise ValueError("GITHUB_REPOSITORY and GH_TOKEN/GITHUB_TOKEN are required.")
    return 0 if restore_latest(repository, args.output) else 2


if __name__ == "__main__":
    raise SystemExit(main())
