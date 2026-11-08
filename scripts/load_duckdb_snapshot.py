"""Replay every approved immutable raw snapshot into DuckDB in extraction order."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import duckdb
from pyarrow import parquet as pq

from toronto_housing_ingestion.config import load_config
from toronto_housing_ingestion.dlt_loader import ensure_duckdb_database, load_scd2
from toronto_housing_ingestion.utils import atomic_write_json, exclusive_lock

try:  # CLI execution (python scripts/load_duckdb_snapshot.py)
    from migrate_legacy_snapshot_artifacts import EXPECTED_LEGACY_RUN_IDS
    from snapshot_archive_backlog import discover_archives, snapshot_order
except ImportError:  # pytest/module execution
    from scripts.migrate_legacy_snapshot_artifacts import EXPECTED_LEGACY_RUN_IDS
    from scripts.snapshot_archive_backlog import discover_archives, snapshot_order

SOURCE_NAMES = ("active_permits", "cleared_permits")


def _select_expected_run_archives(archives: list[Any], expected_run_ids) -> list[Any]:
    """Select exactly the requested legacy runs and fail on missing/ambiguous IDs."""
    expected = {str(run_id) for run_id in expected_run_ids}
    if not expected or any(not run_id.isdigit() for run_id in expected):
        raise ValueError("Historical baseline run IDs must be non-empty numeric IDs.")

    selected: dict[str, Any] = {}
    for archive in archives:
        run_id = str(archive.run_id)
        if run_id not in expected:
            continue
        if run_id in selected:
            raise ValueError(
                f"Historical baseline contains multiple archive attempts for run {run_id}."
            )
        selected[run_id] = archive

    missing = sorted(expected - set(selected), key=int)
    if missing:
        raise RuntimeError(
            "Historical DuckDB baseline archive coverage is incomplete or blocked; "
            f"missing approved legacy runs: {missing}"
        )

    return sorted(selected.values(), key=lambda item: item.order)


def _utc_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid ISO-8601 snapshot timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError("Snapshot timestamps must include a timezone.")
    return parsed.astimezone(UTC)


def _read_json_bytes(value: bytes, label: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{label} must contain a JSON object.")
    return parsed


def _database_path(credentials: str) -> Path:
    path = Path(credentials)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[1] / path
    return path.resolve()


def _ensure_ledger(database: Path) -> None:
    database.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE SCHEMA IF NOT EXISTS raw")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS raw.toronto_snapshot_ledger (
                snapshot_id VARCHAR NOT NULL,
                source_run_id VARCHAR NOT NULL,
                source_run_attempt INTEGER NOT NULL,
                extraction_started_at_utc TIMESTAMPTZ NOT NULL,
                source_name VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                source_commit VARCHAR NOT NULL,
                error VARCHAR,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (snapshot_id, source_name)
            )
            """
        )
        # Upgrade ledgers created by earlier patch versions before using
        # ``excluded.updated_at`` in the ON CONFLICT update below.
        connection.execute(
            "ALTER TABLE raw.toronto_snapshot_ledger "
            "ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS raw.toronto_source_snapshot_metadata (
                source_name VARCHAR PRIMARY KEY,
                extracted_at_utc TIMESTAMPTZ NOT NULL,
                manifest_json VARCHAR NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # Migrate metadata rows written by the first applied archive patch,
        # whose table did not yet include Release identity columns.
        connection.execute(
            "ALTER TABLE raw.toronto_source_snapshot_metadata "
            "ADD COLUMN IF NOT EXISTS release_tag VARCHAR"
        )
        connection.execute(
            "ALTER TABLE raw.toronto_source_snapshot_metadata "
            "ADD COLUMN IF NOT EXISTS asset_name VARCHAR"
        )


def _completed_snapshot_ids(database: Path) -> set[str]:
    """Return identities whose Active and Cleared ledger entries both succeeded."""
    with duckdb.connect(str(database), read_only=True) as connection:
        rows = connection.execute(
            "SELECT snapshot_id, source_name, status FROM raw.toronto_snapshot_ledger"
        ).fetchall()
    grouped: dict[str, dict[str, str]] = {}
    for snapshot_id, source_name, status in rows:
        grouped.setdefault(str(snapshot_id), {})[str(source_name)] = str(status).lower()
    return {
        snapshot_id
        for snapshot_id, statuses in grouped.items()
        if set(statuses) == set(SOURCE_NAMES)
        and all(statuses[name] == "succeeded" for name in SOURCE_NAMES)
    }


def _record_state(
    database: Path,
    *,
    snapshot_id: str,
    run_id: str,
    attempt: int,
    started: datetime,
    source_name: str,
    status: str,
    commit: str,
    error: str | None = None,
) -> None:
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            """
            INSERT INTO raw.toronto_snapshot_ledger
                (snapshot_id, source_run_id, source_run_attempt,
                 extraction_started_at_utc, source_name, status, source_commit, error,
                 updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (snapshot_id, source_name) DO UPDATE SET
                status = excluded.status,
                source_commit = excluded.source_commit,
                error = excluded.error,
                updated_at = excluded.updated_at
            """,
            [
                snapshot_id,
                run_id,
                attempt,
                started,
                source_name,
                status,
                commit,
                error,
                datetime.now(UTC),
            ],
        )


def _assert_replay_order(
    database: Path,
    *,
    snapshot_id: str,
    order: tuple[datetime, int, int],
) -> None:
    with duckdb.connect(str(database), read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT snapshot_id, source_run_id, source_run_attempt,
                   extraction_started_at_utc, source_name, status
            FROM raw.toronto_snapshot_ledger
            """
        ).fetchall()

    current_statuses = {
        old_source: old_status
        for old_id, _old_run, _old_attempt, _old_started, old_source, old_status in rows
        if old_id == snapshot_id
    }
    if set(current_statuses) == set(SOURCE_NAMES) and all(
        str(status).lower() == "succeeded" for status in current_statuses.values()
    ):
        return

    grouped: dict[str, dict[str, Any]] = {}
    for old_id, old_run, old_attempt, old_started, old_source, old_status in rows:
        if old_id == snapshot_id:
            continue
        entry = grouped.setdefault(
            str(old_id),
            {
                "order": snapshot_order(old_started, str(old_run), str(old_attempt)),
                "statuses": {},
            },
        )
        entry["statuses"][str(old_source)] = str(old_status).lower()

    earlier_incomplete: list[tuple[str, dict[str, str], list[str]]] = []
    later_started: list[tuple[str, str, str]] = []
    for old_id, entry in grouped.items():
        statuses = entry["statuses"]
        old_order = entry["order"]
        if old_order < order:
            missing = sorted(set(SOURCE_NAMES) - statuses.keys())
            if missing or any(
                statuses.get(name) != "succeeded" for name in SOURCE_NAMES
            ):
                earlier_incomplete.append((old_id, statuses, missing))
        elif old_order > order:
            later_started.extend(
                (old_id, name, status)
                for name, status in statuses.items()
                if status != "pending"
            )

    if earlier_incomplete:
        raise RuntimeError(
            "DuckDB replay blocked by earlier archived snapshots that do not have "
            "both source loads recorded as succeeded: "
            f"{earlier_incomplete}"
        )
    if later_started:
        raise RuntimeError(
            "Out-of-order DuckDB replay rejected because a newer snapshot has a "
            "non-pending source status (its warehouse write may have committed): "
            f"{later_started}"
        )


def _update_matching_local_manifest(
    manifest_root: Path,
    source_name: str,
    persisted_manifest: dict[str, Any],
) -> Path | None:
    """Mark the matching current-run manifest loaded without losing its raw path.

    The archive-specific manifest stored in DuckDB deliberately has no runner-local
    raw_path. When the original extraction manifest is still present on this runner,
    update it in place so the later ``warehouse_validated`` packager can still find
    the actual Parquet file. Never attach an archive's status to a different local
    snapshot: both extraction timestamp and SHA-256 must match.
    """
    target_timestamp = _utc_timestamp(persisted_manifest["extracted_at_utc"])
    target_digest = str(persisted_manifest.get("sha256", ""))
    source_dir = manifest_root / source_name
    if not source_dir.is_dir():
        return None

    for candidate in sorted(source_dir.glob("*.json"), reverse=True):
        try:
            local = _read_json_bytes(candidate.read_bytes(), str(candidate))
            local_timestamp = _utc_timestamp(local.get("extracted_at_utc", ""))
        except OSError, ValueError:
            continue
        if local_timestamp != target_timestamp or local.get("sha256") != target_digest:
            continue

        raw_value = local.get("raw_path")
        if not raw_value:
            continue
        raw_path = Path(str(raw_value))
        if not raw_path.is_file() or _sha256(raw_path) != target_digest:
            continue

        updated = {**local, **persisted_manifest, "raw_path": str(raw_path)}
        atomic_write_json(candidate, updated)
        return candidate
    return None


def _persist_manifest(
    database: Path,
    manifest_root: Path,
    *,
    source_name: str,
    manifest: dict[str, Any],
    release_tag: str,
    asset_name: str,
) -> None:
    run_id = str(manifest["source_run_id"])
    attempt = str(manifest["source_run_attempt"])
    extracted_at = _utc_timestamp(manifest["extracted_at_utc"])
    filename = (
        extracted_at.strftime("%Y%m%dT%H%M%S.%fZ") + f"_{run_id}_{attempt}_archive.json"
    )
    # Keep the original extraction manifest's real local Parquet path intact.
    # This matters when CD packages its post-load ``warehouse_validated`` bundle.
    _update_matching_local_manifest(manifest_root, source_name, manifest)

    path = manifest_root / source_name / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(path, manifest)

    with duckdb.connect(str(database)) as connection:
        connection.execute(
            """
            INSERT INTO raw.toronto_source_snapshot_metadata
                (source_name, extracted_at_utc, manifest_json, release_tag, asset_name)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (source_name) DO UPDATE SET
                extracted_at_utc = excluded.extracted_at_utc,
                manifest_json = excluded.manifest_json,
                release_tag = excluded.release_tag,
                asset_name = excluded.asset_name,
                updated_at = CURRENT_TIMESTAMP
            """,
            [
                source_name,
                extracted_at,
                json.dumps(manifest, sort_keys=True, default=str),
                release_tag,
                asset_name,
            ],
        )


def _load_archive(
    archive_path: Path,
    *,
    app: Any,
    manifest_root: Path,
    release_tag: str,
    asset_name: str,
) -> None:
    sources = {source.name: source for source in app.sources}
    database = _database_path(app.warehouse_credentials)
    with ZipFile(archive_path) as archive:
        bad_member = archive.testzip()
        if bad_member is not None:
            raise ValueError(f"Snapshot ZIP integrity failed for {bad_member!r}.")
        bundle = _read_json_bytes(archive.read("bundle.json"), "bundle.json")
        if (
            bundle.get("format_version") != 1
            or bundle.get("snapshot_stage") != "raw"
            or bundle.get("complete") is not True
            or bundle.get("approved_for_warehouse") is not True
        ):
            raise ValueError(
                "DuckDB loads only complete, approved raw snapshot archives."
            )

        run_id = str(bundle.get("source_run_id", ""))
        attempt = str(bundle.get("source_run_attempt", "1"))
        commit = str(bundle.get("source_commit", ""))
        if not run_id.isdigit() or not attempt.isdigit() or len(commit) != 40:
            raise ValueError("Raw bundle source identity is invalid.")
        started = _utc_timestamp(bundle.get("extraction_started_at_utc", ""))
        snapshot_id = f"{run_id}:{attempt}"
        order = snapshot_order(started, run_id, attempt)

        bundle_sources = bundle.get("sources")
        if not isinstance(bundle_sources, dict):
            raise ValueError(
                "Approved raw archive has no valid sources metadata object."
            )
        expected_members = {"bundle.json"}
        for source_name in SOURCE_NAMES:
            expected_members.update(
                {f"{source_name}/data.parquet", f"{source_name}/manifest.json"}
            )
        actual_members = set(archive.namelist())
        if actual_members != expected_members:
            raise ValueError(
                "Approved raw archive members differ from the expected five files: "
                f"missing={sorted(expected_members - actual_members)}, "
                f"extra={sorted(actual_members - expected_members)}"
            )

        _assert_replay_order(database, snapshot_id=snapshot_id, order=order)
        failures: list[str] = []
        import tempfile

        with tempfile.TemporaryDirectory(prefix="toronto-duckdb-archive-load-") as temp:
            root = Path(temp)
            prepared_sources: list[tuple[str, Any, dict[str, Any], Path, str]] = []
            for name in SOURCE_NAMES:
                source = sources.get(name)
                summary = bundle_sources.get(name)
                if (
                    source is None
                    or not source.warehouse_load
                    or source.warehouse_strategy.lower() != "scd2"
                    or not isinstance(summary, dict)
                ):
                    raise ValueError(
                        f"Missing or incompatible configured source/archive metadata: {name}"
                    )
                source_dir = root / name
                source_dir.mkdir(parents=True, exist_ok=True)
                parquet_path = source_dir / "data.parquet"
                manifest = _read_json_bytes(
                    archive.read(f"{name}/manifest.json"), f"{name}/manifest.json"
                )
                parquet_path.write_bytes(archive.read(f"{name}/data.parquet"))

                digest = _sha256(parquet_path)
                if digest != summary.get("sha256") or digest != manifest.get("sha256"):
                    raise ValueError(f"{name}: archived Parquet SHA-256 mismatch.")
                parquet = pq.ParquetFile(parquet_path)
                row_count = int(parquet.metadata.num_rows)
                columns = list(parquet.schema_arrow.names)
                if (
                    row_count < 1
                    or row_count != summary.get("row_count")
                    or row_count != manifest.get("row_count")
                ):
                    raise ValueError(
                        f"{name}: archived Parquet row counts disagree or are empty."
                    )
                if sorted(columns) != sorted(summary.get("columns", [])) or sorted(
                    columns
                ) != sorted(manifest.get("columns", [])):
                    raise ValueError(f"{name}: archived Parquet schema mismatch.")
                if (
                    manifest.get("source_name") != name
                    or manifest.get("status") != "success"
                    or manifest.get("warehouse_status") != "not_loaded"
                    or manifest.get("warehouse_eligible") is not True
                    or summary.get("warehouse_eligible") is not True
                ):
                    raise ValueError(
                        f"{name}: source manifest is not approved and ready to load."
                    )
                if (
                    manifest.get("validation_errors")
                    or manifest.get("warehouse_quality_blockers")
                    or summary.get("validation_errors")
                    or summary.get("warehouse_quality_blockers")
                ):
                    raise ValueError(
                        f"{name}: archived manifest/summary contains quality or validation blockers."
                    )
                manifest_extracted_at = _utc_timestamp(
                    manifest.get("extracted_at_utc", "")
                )
                summary_extracted_at = _utc_timestamp(
                    summary.get("extracted_at_utc", "")
                )
                if manifest_extracted_at != summary_extracted_at:
                    raise ValueError(
                        f"{name}: manifest and bundle extraction timestamps disagree."
                    )
                if manifest.get("warehouse_since") != source.warehouse_since:
                    raise ValueError(
                        f"{name}: warehouse date cutoff differs from configuration."
                    )
                if (
                    manifest.get("warehouse_date_column")
                    != source.warehouse_date_column
                ):
                    raise ValueError(
                        f"{name}: warehouse date column differs from configuration."
                    )
                missing = [
                    column
                    for column in source.required_columns
                    if column.lower() not in {item.lower() for item in columns}
                ]
                if missing:
                    raise ValueError(
                        f"{name}: archived Parquet is missing required columns: {missing}"
                    )
                extracted_at = _utc_timestamp(manifest.get("extracted_at_utc", ""))
                if extracted_at < started:
                    raise ValueError(
                        f"{name}: source extraction predates the bundle extraction marker."
                    )
                boundary = str(manifest["extracted_at_utc"])
                prepared_sources.append(
                    (name, source, manifest, parquet_path, boundary)
                )

            # Both Active and Cleared archives are fully validated before the
            # first SCD2 write. A corrupt second source cannot leave the first
            # source partially applied.
            for name, source, manifest, parquet_path, boundary in prepared_sources:
                with duckdb.connect(str(database), read_only=True) as connection:
                    existing = connection.execute(
                        "SELECT status FROM raw.toronto_snapshot_ledger "
                        "WHERE snapshot_id = ? AND source_name = ?",
                        [snapshot_id, name],
                    ).fetchone()
                if existing and existing[0] == "succeeded":
                    print(f"Skipping {name}: archive {snapshot_id} is already loaded.")
                    continue
                _record_state(
                    database,
                    snapshot_id=snapshot_id,
                    run_id=run_id,
                    attempt=int(attempt),
                    started=started,
                    source_name=name,
                    status="loading",
                    commit=commit,
                )

                try:
                    result = load_scd2(
                        credentials=app.warehouse_credentials,
                        snapshot_path=parquet_path,
                        source=source,
                        dataset_name=app.dataset_name,
                        destination_kind="duckdb",
                        boundary_timestamp=boundary,
                    )
                    persisted_manifest = {
                        **manifest,
                        "source_run_id": run_id,
                        "source_run_attempt": attempt,
                        "source_commit": commit,
                        "status": "success",
                        "warehouse_status": "loaded",
                        "warehouse_table": source.warehouse_table,
                        "warehouse_row_count": result.total_rows,
                        "warehouse_current_rows": result.current_rows,
                        "warehouse_loaded_at_utc": datetime.now(UTC).isoformat(),
                        "raw_path": None,
                        "archive_reference": {
                            "release_tag": release_tag,
                            "asset_name": asset_name,
                            "member": f"{name}/data.parquet",
                        },
                    }
                    if result.quarantined_business_keys:
                        persisted_manifest["scd2_quarantine"] = {
                            "reason": "conflicting_scd2_tracked_values",
                            "business_key_count": len(result.quarantined_business_keys),
                            "source_row_count": result.quarantined_row_count,
                            "business_keys": list(result.quarantined_business_keys),
                        }
                    _persist_manifest(
                        database,
                        manifest_root,
                        source_name=name,
                        manifest=persisted_manifest,
                        release_tag=release_tag,
                        asset_name=asset_name,
                    )
                    _record_state(
                        database,
                        snapshot_id=snapshot_id,
                        run_id=run_id,
                        attempt=int(attempt),
                        started=started,
                        source_name=name,
                        status="succeeded",
                        commit=commit,
                    )
                    print(
                        f"Loaded {name} from {release_tag}/{asset_name} "
                        f"with boundary_timestamp={boundary}."
                    )
                except Exception as exc:
                    _record_state(
                        database,
                        snapshot_id=snapshot_id,
                        run_id=run_id,
                        attempt=int(attempt),
                        started=started,
                        source_name=name,
                        status="failed",
                        commit=commit,
                        error=f"{type(exc).__name__}: {exc}"[:4000],
                    )
                    failures.append(f"{name}: {type(exc).__name__}: {exc}")
        if failures:
            raise RuntimeError(
                f"DuckDB archive {snapshot_id} was partially processed; retrying the "
                f"same immutable archive resumes unfinished sources: {'; '.join(failures)}"
            )


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _assert_canonical_replay_baseline(database: Path) -> None:
    """Require an explicitly identified, complete promoted DuckDB baseline.

    A single completed ledger row is not proof that the pre-existing warehouse
    represents the complete historical archive. The repository variables must
    identify the promoted last snapshot and the exact number of snapshots through
    that baseline. The ledger must contain both successful sources for every one.
    """
    canonical_value = os.environ.get(
        "TORONTO_CANONICAL_DUCKDB_PATH", "data/warehouse/toronto.duckdb"
    )
    canonical_path = _database_path(canonical_value)
    if database.resolve() != canonical_path.resolve():
        return

    if os.environ.get("TORONTO_CANONICAL_REPLAY_ENABLED", "").lower() != "true":
        raise RuntimeError(
            "Canonical DuckDB replay is disabled until the isolated historical "
            "rebuild, parity validation, and controlled promotion are complete. "
            "Use a separate DuckDB file for isolated replay."
        )

    baseline_id = os.environ.get("TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_ID", "")
    expected_count_text = os.environ.get(
        "TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_COUNT", ""
    )
    identity_parts = baseline_id.split(":")
    if len(identity_parts) != 2 or not all(part.isdigit() for part in identity_parts):
        raise RuntimeError(
            "TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_ID must be run_id:attempt."
        )
    if not expected_count_text.isdigit() or int(expected_count_text) < 1:
        raise RuntimeError(
            "TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_COUNT must be a positive integer."
        )
    expected_count = int(expected_count_text)

    if not database.is_file():
        raise RuntimeError(
            "Canonical DuckDB baseline is missing; refusing to initialize it from "
            "the raw archive backlog."
        )

    with duckdb.connect(str(database), read_only=True) as connection:
        table_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'raw' AND table_name = 'toronto_snapshot_ledger'"
        ).fetchone()[0]
        if not table_exists:
            raise RuntimeError(
                "Canonical DuckDB has no promoted snapshot ledger. Rebuild and "
                "validate history in an isolated database before enabling replay."
            )
        rows = connection.execute(
            "SELECT snapshot_id, source_run_id, source_run_attempt, "
            "extraction_started_at_utc, source_name, status "
            "FROM raw.toronto_snapshot_ledger"
        ).fetchall()

    grouped: dict[str, dict[str, Any]] = {}
    for snapshot_id, run_id, attempt, started, source_name, status in rows:
        entry = grouped.setdefault(
            str(snapshot_id),
            {
                "order": (started.astimezone(UTC), int(run_id), int(attempt)),
                "statuses": {},
            },
        )
        row_order = (started.astimezone(UTC), int(run_id), int(attempt))
        if entry["order"] != row_order:
            raise RuntimeError(
                f"Inconsistent identity metadata for DuckDB snapshot {snapshot_id}."
            )
        source_key = str(source_name)
        if source_key in entry["statuses"]:
            raise RuntimeError(
                f"Duplicate DuckDB baseline ledger entry for {snapshot_id}/{source_key}."
            )
        if set(entry["statuses"]) - {"active_permits", "cleared_permits"}:
            raise RuntimeError(
                f"Unexpected source name in DuckDB snapshot ledger: {source_key}."
            )
        entry["statuses"][source_key] = str(status).lower()

    baseline = grouped.get(baseline_id)
    if baseline is None:
        raise RuntimeError(
            f"Promoted baseline snapshot {baseline_id} is absent from the DuckDB ledger."
        )
    baseline_order = baseline["order"]
    through_baseline = {
        snapshot_id: entry
        for snapshot_id, entry in grouped.items()
        if entry["order"] <= baseline_order
    }
    completed = {
        snapshot_id
        for snapshot_id, entry in through_baseline.items()
        if set(entry["statuses"]) == {"active_permits", "cleared_permits"}
        and all(
            entry["statuses"].get(name) == "succeeded"
            for name in ("active_permits", "cleared_permits")
        )
    }
    if len(through_baseline) != expected_count or len(completed) != expected_count:
        raise RuntimeError(
            "Promoted DuckDB baseline ledger coverage does not match the declared "
            f"baseline: expected {expected_count} complete snapshots through "
            f"{baseline_id}, found {len(through_baseline)} ledger snapshots and "
            f"{len(completed)} fully completed snapshots."
        )
    if baseline_id not in completed:
        raise RuntimeError(
            f"Promoted baseline snapshot {baseline_id} is not complete for both sources."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("open_data_config.yml"))
    parser.add_argument("--manifest-root", type=Path, default=Path("data/manifests"))
    parser.add_argument("--lock-file", type=Path, default=Path("data/.ingestion.lock"))
    parser.add_argument(
        "--bundle",
        type=Path,
        help="Load one already-downloaded immutable archive (normally use --backlog).",
    )
    parser.add_argument(
        "--backlog",
        action="store_true",
        help="Discover and replay approved monthly Release archives in order.",
    )
    parser.add_argument(
        "--legacy-historical-baseline",
        action="store_true",
        help="Restrict --backlog to the 28 preserved legacy snapshots from Oct 27-Nov 5, 2026.",
    )
    args = parser.parse_args()
    if args.legacy_historical_baseline and not args.backlog:
        parser.error("--legacy-historical-baseline requires --backlog.")
    if args.bundle is not None and args.backlog:
        parser.error("--bundle and --backlog cannot be combined.")
    if args.bundle is None and not args.backlog:
        parser.error("Specify --backlog or --bundle.")

    app = load_config(args.config)
    if app.warehouse_kind != "duckdb":
        raise ValueError(
            "DuckDB archive loader requires TORONTO_WAREHOUSE_KIND=duckdb."
        )

    with exclusive_lock(args.lock_file):
        database = _database_path(app.warehouse_credentials)
        _assert_canonical_replay_baseline(database)
        ensure_duckdb_database(app.warehouse_credentials, app.dataset_name)
        _ensure_ledger(database)

        if args.backlog:
            repository = os.environ.get("GITHUB_REPOSITORY", "")
            if not repository:
                raise ValueError(
                    "GITHUB_REPOSITORY is required for archive backlog discovery."
                )
            import tempfile

            with tempfile.TemporaryDirectory(
                prefix="toronto-snapshot-backlog-"
            ) as temp:
                completed_ids = _completed_snapshot_ids(database)
                # A fresh CD run may already have archived snapshots newer
                # than Nov 5. The initial two-warehouse rebuild must use the
                # same fixed 28-run scope as the Snowflake replay.
                archives = discover_archives(
                    repository,
                    Path(temp),
                    exclude_snapshot_ids=(
                        set() if args.legacy_historical_baseline else completed_ids
                    ),
                )
                if args.legacy_historical_baseline:
                    archives = _select_expected_run_archives(
                        archives, EXPECTED_LEGACY_RUN_IDS
                    )
                    print(
                        f"Selected exactly {len(archives)} legacy historical "
                        "snapshots (Oct 27-Nov 5, 2026)."
                    )
                for item in archives:
                    _load_archive(
                        item.path,
                        app=app,
                        manifest_root=args.manifest_root,
                        release_tag=item.release_tag,
                        asset_name=item.asset_name,
                    )
        else:
            raise ValueError(
                "Direct local bundle loading is intentionally disabled; use --backlog "
                "so provenance, immutable Release identity and chronological order are checked."
            )

    print("DuckDB approved snapshot backlog replay completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
