"""Restore an authoritative DuckDB baseline from a selected GitHub Release.

The default dashboard warehouse retains its verified immutable-recovery fallback.
The separate loading-latest baseline is fail-closed and never substitutes a
possibly different dashboard-serving database when its own asset is unavailable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import duckdb

from scripts import restore_duckdb_recovery

PRODUCTION_TABLES = (
    "raw_active_permits",
    "raw_cleared_permits",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run_gh(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["gh", *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _rolling_asset_available(repository: str, release_tag: str, database_asset: str) -> bool:
    result = subprocess.run(
        [
            "gh",
            "release",
            "view",
            release_tag,
            "--repo",
            repository,
            "--json",
            "assets",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        message = f"{result.stdout}\n{result.stderr}".lower()

        # A missing rolling Release is an allowed reason to use the
        # immutable checkpoint. Authentication, network, and other errors
        # are not treated as evidence that the asset is absent.
        if "404" in message or "not found" in message:
            return False

        raise RuntimeError(
            "Could not inspect warehouse-latest; refusing to guess whether "
            f"its DuckDB asset is unavailable: {result.stderr.strip()}"
        )

    try:
        release = json.loads(result.stdout)
        assets = release["assets"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise RuntimeError(
            "GitHub returned an invalid warehouse-latest asset listing."
        ) from exc

    if not isinstance(assets, list):
        raise RuntimeError(
            "GitHub returned an invalid warehouse-latest asset collection."
        )

    return any(
        isinstance(asset, dict) and asset.get("name") == database_asset
        for asset in assets
    )


def _download_rolling_asset(repository: str, destination: Path, release_tag: str, database_asset: str) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)

    _run_gh(
        "release",
        "download",
        release_tag,
        "--repo",
        repository,
        "--pattern",
        database_asset,
        "--dir",
        str(destination.parent),
    )

    downloaded = destination.parent / database_asset
    if not downloaded.is_file() or downloaded.stat().st_size <= 0:
        raise RuntimeError(
            "warehouse-latest reported a DuckDB asset, but the download "
            "did not produce a non-empty toronto.duckdb."
        )

    if downloaded.resolve() != destination.resolve():
        shutil.move(str(downloaded), str(destination))


def _restore_immutable_checkpoint(
    repository: str,
    destination: Path,
) -> bool:
    return restore_duckdb_recovery.restore_latest(repository, destination)


def _validate_database(path: Path) -> None:
    if not path.is_file() or path.stat().st_size <= 0:
        raise RuntimeError(f"DuckDB production database is missing or empty: {path}")

    try:
        with duckdb.connect(str(path), read_only=True) as connection:
            tables = {
                str(row[0]).lower()
                for row in connection.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'raw'"
                ).fetchall()
            }

            required = set(PRODUCTION_TABLES) | {"toronto_snapshot_ledger"}
            missing = sorted(required - tables)

            if missing:
                raise RuntimeError(
                    f"DuckDB production database is missing raw tables: {missing}"
                )

            for table in PRODUCTION_TABLES:
                total, current = connection.execute(
                    f"SELECT COUNT(*), "
                    f"COUNT(*) FILTER (WHERE _dlt_valid_to IS NULL) "
                    f"FROM raw.{table}"
                ).fetchone()

                if int(total or 0) <= 0 or int(current or 0) <= 0:
                    raise RuntimeError(
                        f"raw.{table} has no current SCD2 production rows."
                    )

            ledger_count = connection.execute(
                "SELECT COUNT(*) FROM raw.toronto_snapshot_ledger"
            ).fetchone()[0]

            if int(ledger_count or 0) <= 0:
                raise RuntimeError("raw.toronto_snapshot_ledger is empty.")

    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"DuckDB production database failed validation: {path}: {exc}"
        ) from exc


def _atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    staged = destination.with_name(destination.name + ".restore.tmp")

    try:
        shutil.copy2(source, staged)

        if _sha256(source) != _sha256(staged):
            raise RuntimeError("Restored DuckDB copy failed SHA-256 verification.")

        staged.replace(destination)
    finally:
        staged.unlink(missing_ok=True)


def restore_production_database(
    repository: str,
    output: Path,
    *,
    release_tag: str = "warehouse-latest",
    database_asset: str = "toronto.duckdb",
) -> str:
    output = output.resolve()

    with tempfile.TemporaryDirectory(
        prefix="toronto-duckdb-production-restore-"
    ) as temporary:
        root = Path(temporary)
        candidate = root / database_asset

        if _rolling_asset_available(repository, release_tag, database_asset):
            _download_rolling_asset(repository, candidate, release_tag, database_asset)

            # A present but invalid rolling asset must fail closed. Do not
            # hide it by silently loading a potentially older checkpoint.
            _validate_database(candidate)
            _atomic_copy(candidate, output)

            print(f"Restored authoritative DuckDB production database from {release_tag}/{database_asset}.")
            return release_tag

        if release_tag != "warehouse-latest":
            raise FileNotFoundError(
                f"{release_tag} has no {database_asset}; refusing to substitute another baseline."
            )

        restored = _restore_immutable_checkpoint(repository, candidate)

        if not restored:
            raise FileNotFoundError(
                "warehouse-latest has no available DuckDB asset and no "
                "verified immutable recovery checkpoint could be restored."
            )

        _validate_database(candidate)
        _atomic_copy(candidate, output)

        print(
            "Restored authoritative DuckDB production database from "
            "the verified immutable recovery checkpoint."
        )
        return "immutable-recovery-checkpoint"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/warehouse/toronto.duckdb"),
    )
    parser.add_argument(
        "--repository",
        default=os.environ.get("GITHUB_REPOSITORY"),
    )
    parser.add_argument("--release-tag", default="warehouse-latest")
    parser.add_argument("--database-asset", default="toronto.duckdb")
    args = parser.parse_args()

    if not args.repository:
        parser.error("--repository or GITHUB_REPOSITORY is required.")

    restore_production_database(
        args.repository, args.output,
        release_tag=args.release_tag, database_asset=args.database_asset,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
