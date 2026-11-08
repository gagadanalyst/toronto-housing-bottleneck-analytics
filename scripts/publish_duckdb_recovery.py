"""Publish an immutable, versioned DuckDB SCD2 recovery checkpoint."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

MAX_ASSET_BYTES = 2_147_483_648


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["gh", *args], check=True, capture_output=True, text=True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _latest_ledger_state(database: Path) -> dict[str, Any] | None:
    with duckdb.connect(str(database), read_only=True) as connection:
        tables = {
            str(row[0]).lower()
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='raw'"
            ).fetchall()
        }
        if "toronto_snapshot_ledger" not in tables:
            return None
        row = connection.execute(
            """
            SELECT source_run_id, source_run_attempt, extraction_started_at_utc,
                   source_commit,
                   COUNT(DISTINCT source_name) AS source_count,
                   COUNT(DISTINCT CASE WHEN source_name IN ('active_permits', 'cleared_permits') THEN source_name END) AS expected_source_count,
                   SUM(CASE WHEN LOWER(status) = 'succeeded' THEN 1 ELSE 0 END) AS succeeded_count,
                   COUNT(*) AS ledger_rows
            FROM raw.toronto_snapshot_ledger
            GROUP BY source_run_id, source_run_attempt, extraction_started_at_utc, source_commit
            ORDER BY extraction_started_at_utc DESC,
                     TRY_CAST(source_run_id AS BIGINT) DESC,
                     source_run_attempt DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None
    return {
        "source_run_id": str(row[0]),
        "source_run_attempt": int(row[1]),
        "extraction_started_at_utc": row[2].astimezone(UTC).isoformat(),
        "source_commit": str(row[3]),
        "source_count": int(row[4]),
        "expected_source_count": int(row[5]),
        "succeeded_count": int(row[6]),
        "ledger_rows": int(row[7]),
    }


def _flatten(value: Any) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    if isinstance(value, list):
        for item in value:
            output.extend(_flatten(item))
    elif isinstance(value, dict):
        output.append(value)
    return output


def _release_assets(repository: str, tag: str) -> tuple[int, list[dict[str, Any]]]:
    release = json.loads(_run("api", f"repos/{repository}/releases/tags/{tag}").stdout)
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
    return release_id, _flatten(pages)


def _ensure_release(repository: str, tag: str) -> None:
    result = subprocess.run(
        ["gh", "release", "view", tag, "--repo", repository],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return
    _run(
        "release",
        "create",
        tag,
        "--repo",
        repository,
        "--title",
        f"Toronto DuckDB recovery {tag[-7:]}",
        "--notes",
        "Immutable versioned DuckDB database checkpoints. No checkpoint asset is overwritten or automatically pruned.",
    )


def _publish_one(repository: str, tag: str, path: Path, asset_name: str) -> None:
    if path.stat().st_size <= 0 or path.stat().st_size >= MAX_ASSET_BYTES:
        raise ValueError(
            f"Recovery asset size is invalid or exceeds 2 GiB: {path.stat().st_size}"
        )
    _release_id, assets = _release_assets(repository, tag)
    matches = [item for item in assets if item.get("name") == asset_name]
    if len(matches) > 1:
        raise RuntimeError(f"Duplicate recovery asset names in {tag}: {asset_name}")
    if matches:
        with tempfile.TemporaryDirectory(prefix="toronto-recovery-check-") as temp:
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
            if not existing.is_file() or _sha256(existing) != _sha256(path):
                raise RuntimeError(
                    f"Existing immutable recovery asset differs: {tag}/{asset_name}"
                )
        return
    if len(assets) >= 1000:
        raise RuntimeError(
            f"Recovery Release asset limit reached for {tag}; no assets were removed."
        )
    _run("release", "upload", tag, str(path), "--repo", repository)
    with tempfile.TemporaryDirectory(prefix="toronto-recovery-verify-") as temp:
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
        if not downloaded.is_file() or _sha256(downloaded) != _sha256(path):
            raise RuntimeError(
                f"Published recovery asset failed verification: {tag}/{asset_name}"
            )


def publish_checkpoint(
    database: Path, repository: str, run_id: str, attempt: str
) -> tuple[str, str] | None:
    if not database.is_file() or database.stat().st_size == 0:
        raise FileNotFoundError(f"DuckDB database is missing or empty: {database}")
    if not run_id.isdigit() or not attempt.isdigit():
        raise ValueError("GitHub run ID and attempt must contain digits only.")
    latest = _latest_ledger_state(database)
    if latest is None:
        print("No snapshot ledger exists yet; no recovery checkpoint was published.")
        return None
    if (
        latest["source_count"] != 2
        or latest["expected_source_count"] != 2
        or latest["succeeded_count"] != 2
        or latest["ledger_rows"] != 2
    ):
        raise RuntimeError(
            "Refusing to publish a DuckDB recovery checkpoint: the latest "
            "snapshot does not have exactly one succeeded ledger row for "
            f"both Active and Cleared sources: {latest}"
        )
    latest_stamp = datetime.fromisoformat(
        latest["extraction_started_at_utc"].replace("Z", "+00:00")
    )
    tag = f"toronto-duckdb-recovery-{latest_stamp.astimezone(UTC):%Y-%m}"
    prefix = f"toronto-duckdb-recovery-{run_id}-{attempt}"
    db_name = f"{prefix}.duckdb"
    manifest_name = f"{prefix}.json"
    metadata = {
        "format_version": 1,
        "checkpoint_run_id": run_id,
        "checkpoint_run_attempt": int(attempt),
        "latest_ledger_snapshot": latest,
        "database_asset": db_name,
        "database_size_bytes": database.stat().st_size,
        "database_sha256": _sha256(database),
        "release_tag": tag,
    }

    with tempfile.TemporaryDirectory(prefix="toronto-duckdb-recovery-publish-") as temp:
        root = Path(temp)
        db_copy = root / db_name
        shutil.copy2(database, db_copy)
        metadata_path = root / manifest_name
        metadata_path.write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        _ensure_release(repository, tag)
        _publish_one(repository, tag, db_copy, db_name)
        _publish_one(repository, tag, metadata_path, manifest_name)

        # Verify the paired assets as downloaded, not merely the upload command result.
        verify_dir = root / "verify"
        verify_dir.mkdir()
        _run(
            "release",
            "download",
            tag,
            "--repo",
            repository,
            "--pattern",
            db_name,
            "--dir",
            str(verify_dir),
        )
        _run(
            "release",
            "download",
            tag,
            "--repo",
            repository,
            "--pattern",
            manifest_name,
            "--dir",
            str(verify_dir),
        )
        downloaded_db = verify_dir / db_name
        downloaded_meta = json.loads(
            (verify_dir / manifest_name).read_text(encoding="utf-8")
        )
        if (
            _sha256(downloaded_db) != metadata["database_sha256"]
            or downloaded_meta != metadata
        ):
            raise RuntimeError(
                "Versioned DuckDB recovery checkpoint pair failed final verification."
            )
    return tag, db_name


def main() -> int:
    database = Path(
        os.environ.get("TORONTO_DUCKDB_PATH", "data/warehouse/toronto.duckdb")
    )
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    if (
        not repository
        or not os.environ.get("GH_TOKEN")
        and not os.environ.get("GITHUB_TOKEN")
    ):
        raise ValueError("GITHUB_REPOSITORY and GH_TOKEN/GITHUB_TOKEN are required.")
    published = publish_checkpoint(database, repository, run_id, attempt)
    if published:
        print(
            f"Published immutable DuckDB recovery checkpoint: {published[0]}/{published[1]}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
