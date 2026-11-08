from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import load_duckdb_snapshot as loader
from scripts import package_snapshot_bundle as packager

EXTRACTED = "2026-11-07T15:00:00+00:00"


def _write_source(root: Path, source_name: str) -> tuple[Path, Path]:
    parquet_path = root / "data" / "raw" / source_name / "data.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist([{"PERMIT_NUM": "P-1"}]), parquet_path)
    digest = loader._sha256(parquet_path)
    manifest = {
        "source_name": source_name,
        "status": "success",
        "warehouse_status": "not_loaded",
        "warehouse_eligible": True,
        "warehouse_quality_blockers": [],
        "validation_errors": [],
        "required_columns": ["PERMIT_NUM"],
        "sha256": digest,
        "row_count": 1,
        "columns": ["PERMIT_NUM"],
        "raw_path": str(parquet_path),
        "extracted_at_utc": EXTRACTED,
        "warehouse_since": None,
        "warehouse_date_column": None,
    }
    manifest_path = (
        root / "data" / "manifests" / source_name / "20261107T15000000000000Z.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return parquet_path, manifest_path


def test_replay_updates_original_manifest_without_erasing_local_parquet_path(tmp_path):
    raw_path, manifest_path = _write_source(tmp_path, "active_permits")
    local = json.loads(manifest_path.read_text(encoding="utf-8"))
    persisted = {
        **local,
        "status": "success",
        "warehouse_status": "loaded",
        "warehouse_table": "RAW_ACTIVE_PERMITS",
        "raw_path": None,
        "archive_reference": {"release_tag": "toronto-raw-snapshot-archive-2026-11"},
    }

    updated = loader._update_matching_local_manifest(
        tmp_path / "data" / "manifests", "active_permits", persisted
    )

    assert updated == manifest_path
    result = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert result["warehouse_status"] == "loaded"
    assert Path(result["raw_path"]) == raw_path
    assert (
        result["archive_reference"]["release_tag"]
        == "toronto-raw-snapshot-archive-2026-11"
    )


def test_warehouse_validated_packager_chooses_loaded_manifest_with_existing_raw_path(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    for source_name in ("active_permits", "cleared_permits"):
        raw_path, manifest_path = _write_source(tmp_path, source_name)
        local = json.loads(manifest_path.read_text(encoding="utf-8"))
        local.update(
            {"warehouse_status": "loaded", "warehouse_table": source_name.upper()}
        )
        manifest_path.write_text(json.dumps(local), encoding="utf-8")
        # Archive-only DuckDB metadata has a later lexical filename but no local raw path.
        archive_copy = {
            **local,
            "raw_path": None,
            "archive_reference": {"asset_name": "older-archive.zip"},
        }
        archive_path = manifest_path.with_name("zzzz_archive_metadata.json")
        archive_path.write_text(json.dumps(archive_copy), encoding="utf-8")
        assert raw_path.is_file()

    monkeypatch.setenv("SNAPSHOT_EXTRACTION_STARTED_AT", "2026-11-07T14:59:00+00:00")
    monkeypatch.setenv("GITHUB_RUN_ID", "900")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("SNAPSHOT_SOURCE_COMMIT", "a" * 40)
    output = tmp_path / "warehouse-validated.zip"

    packager.package_bundle(output, stage="warehouse_validated")

    with ZipFile(output) as archive:
        assert archive.testzip() is None
        bundle = json.loads(archive.read("bundle.json"))
        assert bundle["snapshot_stage"] == "warehouse_validated"
        for source_name in ("active_permits", "cleared_permits"):
            assert f"{source_name}/data.parquet" in archive.namelist()
            manifest = json.loads(archive.read(f"{source_name}/manifest.json"))
            assert manifest["warehouse_status"] == "loaded"
            assert manifest["raw_path"]
