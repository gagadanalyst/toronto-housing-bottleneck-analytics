"""Tests for permanent-Release input to the isolated Snowflake SCD2 replay."""

from __future__ import annotations

import hashlib
import io
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from zipfile import ZIP_STORED, ZipFile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Keep these tests runnable without the optional Snowflake connector extra.
if "snowflake.connector" not in sys.modules:
    try:
        import snowflake.connector  # noqa: F401
    except ModuleNotFoundError:
        snowflake_package = ModuleType("snowflake")
        snowflake_package.__path__ = []
        connector_module = ModuleType("snowflake.connector")

        def _no_live_connection(**kwargs):
            raise AssertionError(
                "Historical replay unit tests must not connect to Snowflake"
            )

        connector_module.connect = _no_live_connection
        snowflake_package.connector = connector_module
        sys.modules["snowflake"] = snowflake_package
        sys.modules["snowflake.connector"] = connector_module

from scripts import snowflake_scd2_historical_replay as replay
from scripts.snapshot_archive_backlog import SnapshotArchive

ATTEMPTS = {"40491722684": "2", "40679228717": "4"}
COMMIT = "a" * 40


def _expected_archives(tmp_path: Path) -> list[SnapshotArchive]:
    result = []
    base = datetime(2026, 10, 27, 14, 0, tzinfo=UTC)
    for index, run_id in enumerate(replay.RUN_IDS):
        attempt = ATTEMPTS.get(run_id, "1")
        started = base + timedelta(minutes=index * 3)
        extracted = started + timedelta(seconds=2)
        stamp = extracted.isoformat()
        started_stamp = started.isoformat()
        sources = {}
        members = {}
        for source_name in replay.SOURCE_NAMES:
            table = pa.table(
                {
                    "PERMIT_NUM": [f"P-{index}-{source_name}"],
                    "REVISION_NUM": ["00"],
                    "PERMIT_TYPE": ["Test Permit"],
                }
            )
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            parquet_bytes = buffer.getvalue()
            digest = hashlib.sha256(parquet_bytes).hexdigest()
            columns = list(table.schema.names)
            manifest = {
                "source_name": source_name,
                "source_run_id": run_id,
                "source_run_attempt": attempt,
                "source_commit": COMMIT,
                "status": "success",
                "warehouse_status": "not_loaded",
                "warehouse_eligible": True,
                "warehouse_quality_blockers": [],
                "validation_errors": [],
                "warehouse_since": None,
                "warehouse_date_column": None,
                "raw_size_bytes": len(parquet_bytes),
                "sha256": digest,
                "row_count": 1,
                "columns": columns,
                "extracted_at_utc": stamp,
            }
            manifest_member = f"{source_name}/manifest.json"
            data_member = f"{source_name}/data.parquet"
            members[manifest_member] = json.dumps(manifest).encode()
            members[data_member] = parquet_bytes
            sources[source_name] = {
                "snapshot": data_member,
                "manifest": manifest_member,
                "sha256": digest,
                "row_count": 1,
                "columns": columns,
                "extracted_at_utc": stamp,
                "warehouse_since": None,
                "warehouse_date_column": None,
                "warehouse_eligible": True,
                "warehouse_quality_blockers": [],
                "validation_errors": [],
            }

        bundle = {
            "format_version": 1,
            "snapshot_stage": "raw",
            "complete": True,
            "approved_for_warehouse": True,
            "source_run_id": run_id,
            "source_run_attempt": attempt,
            "source_commit": COMMIT,
            "extraction_started_at_utc": started_stamp,
            "sources": sources,
        }
        path = tmp_path / f"{run_id}-{attempt}.zip"
        with ZipFile(path, "w", compression=ZIP_STORED) as archive:
            archive.writestr("bundle.json", json.dumps(bundle))
            for name, value in members.items():
                archive.writestr(name, value)
        release_tag = f"toronto-raw-snapshot-archive-{started:%Y-%m}"
        asset_name = f"toronto-raw-snapshot-bundle-{run_id}-{attempt}.zip"
        result.append(
            SnapshotArchive(
                release_tag=release_tag,
                asset_name=asset_name,
                path=path,
                bundle=bundle,
                extracted_at=started,
                run_id=run_id,
                attempt=attempt,
            )
        )
    return result


def _sources():
    required = ("PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE")
    return {
        name: SimpleNamespace(
            name=name,
            required_columns=required,
            warehouse_since=None,
            warehouse_date_column=None,
        )
        for name in replay.SOURCE_NAMES
    }


def test_selection_requires_exact_run_set_and_preserves_real_attempts(tmp_path):
    archives = _expected_archives(tmp_path)
    archives.append(
        SimpleNamespace(
            run_id="99999999999", attempt="1", order=(datetime.now(UTC), 1, 1)
        )
    )

    selected = replay._select_expected_archives(archives)

    assert len(selected) == 28
    assert [item.run_id for item in selected] == list(replay.RUN_IDS)
    assert {
        item.run_id: item.attempt for item in selected if item.run_id in ATTEMPTS
    } == ATTEMPTS


def test_selection_rejects_missing_archive(tmp_path):
    archives = _expected_archives(tmp_path)
    archives = [item for item in archives if item.run_id != replay.RUN_IDS[0]]

    with pytest.raises(RuntimeError, match="missing approved monthly Release runs"):
        replay._select_expected_archives(archives)


def test_selection_rejects_multiple_attempts_for_expected_run(tmp_path):
    archives = _expected_archives(tmp_path)
    original = next(item for item in archives if item.run_id == replay.RUN_IDS[0])
    duplicate = SimpleNamespace(
        run_id=original.run_id,
        attempt="9",
        order=(original.extracted_at + timedelta(seconds=1), int(original.run_id), 9),
    )
    archives.append(duplicate)

    with pytest.raises(ValueError, match="multiple Release archive attempts"):
        replay._select_expected_archives(archives)


def test_selection_rejects_single_archive_with_wrong_attempt(tmp_path):
    archives = _expected_archives(tmp_path)
    run_id, expected_attempt = next(iter(ATTEMPTS.items()))
    index = next(i for i, item in enumerate(archives) if item.run_id == run_id)
    original = archives[index]
    wrong_attempt = "1"
    assert wrong_attempt != expected_attempt
    archives[index] = SimpleNamespace(
        release_tag=original.release_tag,
        asset_name=original.asset_name,
        path=original.path,
        bundle=original.bundle,
        extracted_at=original.extracted_at,
        run_id=original.run_id,
        attempt=wrong_attempt,
        order=original.order,
    )

    with pytest.raises(ValueError, match=f"expected attempt {expected_attempt}"):
        replay._select_expected_archives(archives)


def test_replay_preflight_accepts_migrated_raw_not_loaded_bundles(tmp_path):
    archives = _expected_archives(tmp_path)
    prepared = replay._extract_and_validate_bundles(
        archives, tmp_path / "prepared", _sources()
    )

    assert len(prepared["active_permits"]) == 28
    assert len(prepared["cleared_permits"]) == 28
    assert prepared["active_permits"][2].run_id == "40491722684"
    assert prepared["active_permits"][2].run_attempt == "2"
    assert prepared["active_permits"][13].run_id == "40679228717"
    assert prepared["active_permits"][13].run_attempt == "4"
    assert all(
        item.manifest["warehouse_status"] == "not_loaded"
        for rows in prepared.values()
        for item in rows
    )


def test_replay_preflight_rejects_legacy_loaded_manifest_status(tmp_path):
    archives = _expected_archives(tmp_path)
    first = archives[0]
    replacement = tmp_path / "loaded-status.zip"
    with (
        ZipFile(first.path) as source,
        ZipFile(replacement, "w", compression=ZIP_STORED) as target,
    ):
        for member in source.namelist():
            payload = source.read(member)
            if member == "active_permits/manifest.json":
                manifest = json.loads(payload)
                manifest["warehouse_status"] = "loaded"
                payload = json.dumps(manifest).encode()
            target.writestr(member, payload)
    archives[0] = SnapshotArchive(
        release_tag=first.release_tag,
        asset_name=first.asset_name,
        path=replacement,
        bundle=first.bundle,
        extracted_at=first.extracted_at,
        run_id=first.run_id,
        attempt=first.attempt,
    )

    with pytest.raises(ValueError, match="raw snapshot is not complete, eligible"):
        replay._extract_and_validate_bundles(
            archives, tmp_path / "prepared", _sources()
        )
