from __future__ import annotations

import io
import json
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZIP_STORED, ZipFile

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.migrate_legacy_snapshot_artifacts import migrate_legacy_bundle

RUN_ID = "40470002594"
ATTEMPT = "1"
COMMIT = "a" * 40
STARTED = "2026-10-27T14:00:00+00:00"
EXTRACTED = "2026-10-27T14:00:02+00:00"


def _legacy_bundle(
    path: Path, *, omit_cleared: bool = False, corrupt_cleared: bool = False
) -> Path:
    members: dict[str, bytes] = {}
    summaries = {}
    for name, row in {
        "active_permits": {
            "PERMIT_NUM": "P-1",
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New House",
        },
        "cleared_permits": {
            "PERMIT_NUM": "P-2",
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New House",
        },
    }.items():
        if omit_cleared and name == "cleared_permits":
            continue
        if corrupt_cleared and name == "cleared_permits":
            parquet_bytes = b"not parquet bytes"
            columns = []
            row_count = 0
        else:
            buffer = io.BytesIO()
            pq.write_table(pa.Table.from_pylist([row]), buffer)
            parquet_bytes = buffer.getvalue()
            table = pq.read_table(io.BytesIO(parquet_bytes))
            columns = list(table.schema.names)
            row_count = 1
        import hashlib

        digest = hashlib.sha256(parquet_bytes).hexdigest()
        manifest = {
            "source_name": name,
            "status": "success",
            "warehouse_status": "loaded",
            "sha256": digest,
            "row_count": row_count,
            "columns": columns,
            "warehouse_since": None,
            "warehouse_date_column": None,
            "extracted_at_utc": EXTRACTED,
            "quality_warnings": [],
        }
        members[f"{name}/data.parquet"] = parquet_bytes
        members[f"{name}/manifest.json"] = json.dumps(manifest).encode()
        summaries[name] = {
            "sha256": digest,
            "row_count": row_count,
            "columns": columns,
            "extracted_at_utc": EXTRACTED,
            "warehouse_since": None,
            "warehouse_date_column": None,
        }
    bundle = {
        "format_version": 1,
        "source_run_id": RUN_ID,
        "source_commit": COMMIT,
        "extraction_started_at_utc": STARTED,
        "sources": summaries,
    }
    members["bundle.json"] = json.dumps(bundle).encode()
    with ZipFile(path, "w", compression=ZIP_STORED) as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
    return path


def _sources():
    required = ("PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE")
    return {
        name: SimpleNamespace(
            name=name,
            required_columns=required,
            warehouse_since=None,
            warehouse_date_column=None,
            warehouse_strategy="scd2",
            warehouse_load=True,
            min_row_count=0,
            max_row_change_percent=30.0,
        )
        for name in ("active_permits", "cleared_permits")
    }


def test_migration_normalizes_legacy_successful_bundle(tmp_path: Path):
    legacy = _legacy_bundle(tmp_path / "legacy.zip")
    output = tmp_path / "raw.zip"
    bundle = migrate_legacy_bundle(
        legacy,
        output,
        run_id=RUN_ID,
        attempt=ATTEMPT,
        commit=COMMIT,
        sources=_sources(),
    )
    assert bundle["snapshot_stage"] == "raw"
    assert bundle["complete"] is True
    assert bundle["approved_for_warehouse"] is True
    assert bundle["source_run_attempt"] == ATTEMPT
    with ZipFile(output) as archive:
        active = json.loads(archive.read("active_permits/manifest.json"))
        assert active["warehouse_status"] == "not_loaded"
        assert active["warehouse_eligible"] is True
        assert active["raw_path"] is None


def test_migration_retains_good_source_when_other_parquet_is_corrupt(tmp_path: Path):
    legacy = _legacy_bundle(tmp_path / "legacy-corrupt.zip", corrupt_cleared=True)
    output = tmp_path / "partial.zip"
    bundle = migrate_legacy_bundle(
        legacy,
        output,
        run_id=RUN_ID,
        attempt=ATTEMPT,
        commit=COMMIT,
        sources=_sources(),
    )
    assert bundle["complete"] is False
    assert bundle["approved_for_warehouse"] is False
    assert bundle["sources"]["active_permits"]["warehouse_eligible"] is True
    assert bundle["sources"]["cleared_permits"]["warehouse_eligible"] is False
    with ZipFile(output) as archive:
        assert archive.read("active_permits/data.parquet")
        assert archive.read("cleared_permits/data.parquet") == b"not parquet bytes"


def test_migration_retains_one_source_when_other_member_missing(tmp_path: Path):
    legacy = _legacy_bundle(tmp_path / "legacy-missing.zip", omit_cleared=True)
    output = tmp_path / "partial.zip"
    bundle = migrate_legacy_bundle(
        legacy,
        output,
        run_id=RUN_ID,
        attempt=ATTEMPT,
        commit=COMMIT,
        sources=_sources(),
    )
    assert bundle["complete"] is False
    assert "cleared_permits" in bundle["missing_sources"]
    with ZipFile(output) as archive:
        assert "active_permits/data.parquet" in archive.namelist()
        assert "cleared_permits/data.parquet" not in archive.namelist()
        assert "cleared_permits/manifest.json" in archive.namelist()


def test_migration_retains_source_parquet_when_bundle_summary_is_missing(
    tmp_path: Path,
):
    original = _legacy_bundle(tmp_path / "legacy.zip")
    rewritten = tmp_path / "legacy-missing-summary.zip"
    with (
        ZipFile(original) as source,
        ZipFile(rewritten, "w", compression=ZIP_STORED) as target,
    ):
        for name in source.namelist():
            payload = source.read(name)
            if name == "bundle.json":
                bundle = json.loads(payload)
                bundle["sources"].pop("cleared_permits")
                payload = json.dumps(bundle).encode()
            target.writestr(name, payload)

    output = tmp_path / "migrated.zip"
    bundle = migrate_legacy_bundle(
        rewritten,
        output,
        run_id=RUN_ID,
        attempt=ATTEMPT,
        commit=COMMIT,
        sources=_sources(),
    )

    assert bundle["complete"] is False
    assert bundle["approved_for_warehouse"] is False
    assert bundle["sources"]["active_permits"]["warehouse_eligible"] is True
    assert bundle["sources"]["cleared_permits"]["has_snapshot"] is True
    with ZipFile(output) as archive:
        assert archive.read("cleared_permits/data.parquet")
        manifest = json.loads(archive.read("cleared_permits/manifest.json"))
        assert manifest["warehouse_eligible"] is False
        assert manifest["validation_errors"]


def test_migration_blocks_manifest_bundle_timestamp_disagreement(tmp_path: Path):
    original = _legacy_bundle(tmp_path / "legacy.zip")
    rewritten = tmp_path / "legacy-timestamp-mismatch.zip"
    with (
        ZipFile(original) as source,
        ZipFile(rewritten, "w", compression=ZIP_STORED) as target,
    ):
        for name in source.namelist():
            payload = source.read(name)
            if name == "bundle.json":
                bundle = json.loads(payload)
                bundle["sources"]["active_permits"]["extracted_at_utc"] = (
                    "2026-10-27T14:00:03+00:00"
                )
                payload = json.dumps(bundle).encode()
            target.writestr(name, payload)

    output = tmp_path / "migrated.zip"
    bundle = migrate_legacy_bundle(
        rewritten,
        output,
        run_id=RUN_ID,
        attempt=ATTEMPT,
        commit=COMMIT,
        sources=_sources(),
    )

    assert bundle["complete"] is False
    assert bundle["approved_for_warehouse"] is False
    assert bundle["sources"]["active_permits"]["warehouse_eligible"] is False


def test_migration_blocks_excessive_change_from_previous_snapshot(tmp_path: Path):
    legacy = _legacy_bundle(tmp_path / "legacy.zip")
    output = tmp_path / "migrated.zip"
    previous = {"active_permits": 10, "cleared_permits": 1}
    bundle = migrate_legacy_bundle(
        legacy,
        output,
        run_id=RUN_ID,
        attempt=ATTEMPT,
        commit=COMMIT,
        sources=_sources(),
        previous_row_counts=previous,
    )

    assert bundle["complete"] is True
    assert bundle["approved_for_warehouse"] is False
    assert bundle["sources"]["active_permits"]["warehouse_eligible"] is False
    assert any(
        "row_count_change_exceeds_configured_limit" in item
        for item in bundle["sources"]["active_permits"]["warehouse_quality_blockers"]
    )
    assert bundle["sources"]["cleared_permits"]["warehouse_eligible"] is True
    assert previous["active_permits"] == 10  # blocked snapshot did not advance baseline
