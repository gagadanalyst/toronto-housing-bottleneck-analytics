import json
from pathlib import Path
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import package_snapshot_bundle as packager


def _source(tmp_path: Path, name: str, *, corrupt: bool = False) -> Path:
    raw = tmp_path / "data" / "raw" / name / "data.parquet"
    raw.parent.mkdir(parents=True)
    if corrupt:
        raw.write_bytes(b"not parquet")
    else:
        pq.write_table(pa.Table.from_pylist([{"permit_num": "P-1"}]), raw)
    manifest = {
        "source_name": name,
        "status": "success",
        "warehouse_status": "not_loaded",
        "warehouse_eligible": True,
        "warehouse_quality_blockers": [],
        "required_columns": ["permit_num"],
        "sha256": packager._sha256(raw),
        "row_count": 1,
        "columns": ["permit_num"],
        "raw_path": str(raw),
        "extracted_at_utc": "2026-11-07T15:00:00+00:00",
    }
    manifest_path = tmp_path / "data" / "manifests" / name / "20261107T150000Z.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return raw


def test_partial_package_retains_good_source_when_other_parquet_is_corrupt(
    monkeypatch, tmp_path
):
    monkeypatch.chdir(tmp_path)
    _source(tmp_path, "active_permits")
    _source(tmp_path, "cleared_permits", corrupt=True)
    monkeypatch.setenv("SNAPSHOT_EXTRACTION_STARTED_AT", "2026-11-07T14:59:00+00:00")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("SNAPSHOT_SOURCE_COMMIT", "a" * 40)

    output = tmp_path / "bundle.zip"
    packager.package_bundle(output, stage="raw", allow_partial=True)

    with ZipFile(output) as archive:
        bundle = json.loads(archive.read("bundle.json"))
        assert "active_permits/data.parquet" in archive.namelist()
        assert "cleared_permits/data.parquet" in archive.namelist()
        assert bundle["complete"] is False
        assert bundle["approved_for_warehouse"] is False
        assert "cleared_permits" in bundle["source_failures"]


def test_quality_blocked_complete_bundle_is_retained_but_not_approved(
    monkeypatch, tmp_path
):
    monkeypatch.chdir(tmp_path)
    for name in ("active_permits", "cleared_permits"):
        _source(tmp_path, name)
        manifest_path = next((tmp_path / "data" / "manifests" / name).glob("*.json"))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["warehouse_eligible"] = False
        manifest["warehouse_quality_blockers"] = ["row_count_change_exceeded"]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setenv("SNAPSHOT_EXTRACTION_STARTED_AT", "2026-11-07T14:59:00+00:00")
    monkeypatch.setenv("GITHUB_RUN_ID", "124")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("SNAPSHOT_SOURCE_COMMIT", "b" * 40)

    output = tmp_path / "quality-blocked.zip"
    packager.package_bundle(output, stage="raw", allow_partial=True)
    with ZipFile(output) as archive:
        bundle = json.loads(archive.read("bundle.json"))
    assert bundle["complete"] is True
    assert bundle["approved_for_warehouse"] is False
