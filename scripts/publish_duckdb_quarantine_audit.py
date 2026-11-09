"""Publish rejected DuckDB quarantine rows to a separate immutable Release."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from collections import Counter
from datetime import UTC
from pathlib import Path

import duckdb
import pyarrow.parquet as pq


def _run(*args: str, input_text: str | None = None) -> str:
    result = subprocess.run(
        ["gh", *args],
        input=input_text,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ensure_release(repository: str, tag: str) -> None:
    result = subprocess.run(
        ["gh", "release", "view", tag, "--repo", repository],
        check=False,
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
        f"Toronto SCD2 quarantine audit {tag[-7:]}",
        "--notes",
        "Immutable audit exports of newly quarantined DuckDB rows from rejected candidates. "
        "These assets are separate from production DuckDB recovery checkpoints and are "
        "never restored automatically into the production warehouse.",
        "--latest=false",
    )


def _existing_asset(repository: str, tag: str, name: str) -> bool:
    release = json.loads(_run("api", f"repos/{repository}/releases/tags/{tag}"))
    release_id = int(release["id"])
    pages = json.loads(
        _run(
            "api",
            "--paginate",
            "--slurp",
            f"repos/{repository}/releases/{release_id}/assets?per_page=100",
        )
        or "[]"
    )
    assets = [asset for page in pages for asset in page]
    matches = [asset for asset in assets if asset.get("name") == name]
    if len(matches) > 1:
        raise RuntimeError(f"Quarantine Release has duplicate asset names: {name}.")
    return bool(matches)


def publish(database: Path, repository: str, run_id: str, attempt: str) -> int:
    baseline_ids = set(
        json.loads(os.environ.get("TORONTO_QUARANTINE_BASELINE_SNAPSHOT_IDS", "[]"))
    )
    with duckdb.connect(str(database), read_only=True) as connection:
        exists = int(
            connection.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema='raw' AND table_name='toronto_scd2_quarantine'"
            ).fetchone()[0]
        )
        if not exists:
            print("No quarantine table exists in the audit copy; nothing to publish.")
            return 0
        if baseline_ids:
            placeholders = ",".join("?" for _ in baseline_ids)
            query = (
                "SELECT * FROM raw.toronto_scd2_quarantine "
                f"WHERE snapshot_id NOT IN ({placeholders}) "
                "ORDER BY snapshot_id, source_name, row_ordinal, reason"
            )
            table = connection.execute(query, sorted(baseline_ids)).fetch_arrow_table()
        else:
            table = connection.execute(
                "SELECT * FROM raw.toronto_scd2_quarantine "
                "ORDER BY snapshot_id, source_name, row_ordinal, reason"
            ).fetch_arrow_table()

    count = table.num_rows
    if not count:
        print("No new quarantine rows were detected; no audit Release is needed.")
        return 0

    required = {
        "quarantine_id",
        "snapshot_id",
        "source_name",
        "source_run_id",
        "source_run_attempt",
        "asset_name",
        "parquet_sha256",
        "row_ordinal",
        "raw_record_json",
        "reason",
    }
    missing = required - set(table.column_names)
    if missing:
        raise RuntimeError(
            f"Quarantine export is missing provenance/payload fields: {sorted(missing)}"
        )
    null_payloads = sum(value is None for value in table["raw_record_json"].to_pylist())
    if null_payloads:
        raise RuntimeError(
            f"Quarantine export contains {null_payloads} row(s) without original payloads."
        )

    extracted_values = [
        value for value in table["extracted_at_utc"].to_pylist() if value is not None
    ]
    if not extracted_values:
        raise RuntimeError("Quarantine export has no immutable extraction timestamps.")
    audit_created_at = min(extracted_values)
    if audit_created_at.tzinfo is None:
        raise RuntimeError("Quarantine extraction timestamp has no timezone.")
    tag = f"toronto-quarantine-audit-{audit_created_at.astimezone(UTC):%Y-%m}"
    parquet_name = f"toronto-scd2-quarantine-{run_id}-{attempt}.parquet"
    manifest_name = f"toronto-scd2-quarantine-{run_id}-{attempt}.json"

    with tempfile.TemporaryDirectory(prefix="toronto-quarantine-release-") as temp:
        root = Path(temp)
        parquet_path = root / parquet_name
        manifest_path = root / manifest_name
        pq.write_table(table, parquet_path, compression="zstd")
        reason_counts = dict(
            Counter(str(value) for value in table["reason"].to_pylist())
        )
        manifest = {
            "format_version": 1,
            "created_at_utc": audit_created_at.astimezone(UTC).isoformat(),
            "workflow_run_id": run_id,
            "workflow_run_attempt": attempt,
            "quarantine_row_count": count,
            "quarantine_parquet_asset": parquet_name,
            "quarantine_parquet_sha256": _sha256(parquet_path),
            "snapshot_ids": sorted(
                {str(value) for value in table["snapshot_id"].to_pylist()}
            ),
            "sources": sorted(
                {str(value) for value in table["source_name"].to_pylist()}
            ),
            "reason_counts": reason_counts,
        }
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _ensure_release(repository, tag)
        for path in (parquet_path, manifest_path):
            if _existing_asset(repository, tag, path.name):
                verify_dir = root / f"verify-{path.stem}"
                verify_dir.mkdir()
                _run(
                    "release",
                    "download",
                    tag,
                    "--repo",
                    repository,
                    "--pattern",
                    path.name,
                    "--dir",
                    str(verify_dir),
                )
                existing = verify_dir / path.name
                if not existing.is_file() or _sha256(existing) != _sha256(path):
                    raise RuntimeError(
                        f"Immutable quarantine audit asset already exists with different bytes: {path.name}"
                    )
                continue
            _run("release", "upload", tag, str(path), "--repo", repository)

        verify_dir = root / "final-verify"
        verify_dir.mkdir()
        for path in (parquet_path, manifest_path):
            _run(
                "release",
                "download",
                tag,
                "--repo",
                repository,
                "--pattern",
                path.name,
                "--dir",
                str(verify_dir),
            )
            downloaded = verify_dir / path.name
            if not downloaded.is_file() or _sha256(downloaded) != _sha256(path):
                raise RuntimeError(
                    f"Published quarantine audit asset failed verification: {path.name}"
                )

    print(
        f"Published {count:,} new quarantine row(s) to immutable audit Release "
        f"{tag}; production recovery checkpoints were not changed."
    )
    return count


def main() -> int:
    database = Path(os.environ.get("TORONTO_QUARANTINE_AUDIT_DUCKDB_PATH", ""))
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    if not database.is_file():
        raise FileNotFoundError(f"DuckDB quarantine audit copy is missing: {database}")
    if not repository or not run_id.isdigit() or not attempt.isdigit():
        raise ValueError("Repository and numeric GitHub run ID/attempt are required.")
    if not os.environ.get("GH_TOKEN") and not os.environ.get("GITHUB_TOKEN"):
        raise ValueError(
            "GH_TOKEN or GITHUB_TOKEN is required to publish quarantine audit."
        )
    publish(database, repository, run_id, attempt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
