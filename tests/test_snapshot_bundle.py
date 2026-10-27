"""Offline validation tests for the reusable Parquet snapshot bundle."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZIP_STORED, ZipFile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.package_snapshot_bundle import package_bundle
from scripts.snowflake_snapshot_baseline import _validate_and_extract

SOURCE_NAMES = ("active_permits", "cleared_permits")
STARTED_AT = "2026-10-26T15:00:00+00:00"
EXTRACTED_AT = "2026-10-26T15:01:00+00:00"
RUN_ID = "123456789"
COMMIT = "a" * 40

FIELDS = {
    "active_permits": {
        "PERMIT_NUM": "21 000001 BLD",
        "REVISION_NUM": "00",
        "PERMIT_TYPE": "New Houses",
        "APPLICATION_DATE": "2021-05-14",
        "ISSUED_DATE": "2021-06-14",
        "STATUS": "Open",
        "GEO_ID": "00123456",
    },
    "cleared_permits": {
        "PERMIT_NUM": "21 000002 BLD",
        "REVISION_NUM": "00",
        "PERMIT_TYPE": "New Houses",
        "APPLICATION_DATE": "2021-05-15",
        "ISSUED_DATE": "2021-06-15",
        "COMPLETED_DATE": "2021-07-15",
    },
}

REQUIRED_COLUMNS = {
    "active_permits": (
        "PERMIT_NUM",
        "REVISION_NUM",
        "PERMIT_TYPE",
        "APPLICATION_DATE",
        "ISSUED_DATE",
        "STATUS",
        "GEO_ID",
    ),
    "cleared_permits": (
        "PERMIT_NUM",
        "REVISION_NUM",
        "PERMIT_TYPE",
        "APPLICATION_DATE",
        "ISSUED_DATE",
        "COMPLETED_DATE",
    ),
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sources() -> dict:
    return {
        name: SimpleNamespace(
            name=name,
            required_columns=REQUIRED_COLUMNS[name],
            warehouse_since="2020-01-01",
            warehouse_date_column="APPLICATION_DATE",
        )
        for name in SOURCE_NAMES
    }


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def _prepare_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    omit_columns: dict[str, set[str]] | None = None,
) -> Path:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SNAPSHOT_EXTRACTION_STARTED_AT", STARTED_AT)
    monkeypatch.setenv("GITHUB_RUN_ID", RUN_ID)
    monkeypatch.setenv("SNAPSHOT_SOURCE_RUN_ID", RUN_ID)
    monkeypatch.setenv("SNAPSHOT_SOURCE_COMMIT", COMMIT)

    omit_columns = omit_columns or {}
    for source_name in SOURCE_NAMES:
        raw_path = (
            Path("data/raw")
            / source_name
            / "ingest_date=2026-10-26"
            / "run_id=synthetic"
            / "data.parquet"
        )
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        row = {
            key: [value]
            for key, value in FIELDS[source_name].items()
            if key not in omit_columns.get(source_name, set())
        }
        pq.write_table(pa.table(row), raw_path)
        columns = list(pq.ParquetFile(raw_path).schema_arrow.names)
        manifest = {
            "source_name": source_name,
            "status": "success",
            "warehouse_status": "loaded",
            "raw_path": str(raw_path),
            "sha256": _sha256(raw_path),
            "row_count": 1,
            "columns": columns,
            "warehouse_since": "2020-01-01",
            "warehouse_date_column": "APPLICATION_DATE",
            "resource_id": f"resource-{source_name}",
            "extracted_at_utc": EXTRACTED_AT,
        }
        _write_json(Path("data/manifests") / source_name / "synthetic.json", manifest)

    output = tmp_path / "bundle.zip"
    package_bundle(output)
    return output


def _rewrite_bundle(
    original: Path,
    destination: Path,
    *,
    replacements: dict[str, bytes] | None = None,
    remove: set[str] | None = None,
) -> Path:
    replacements = replacements or {}
    remove = remove or set()
    with (
        ZipFile(original, "r") as source,
        ZipFile(destination, "w", compression=ZIP_STORED) as target,
    ):
        for name in source.namelist():
            if name in remove:
                continue
            payload = replacements.get(name, source.read(name))
            target.writestr(name, payload)
    return destination


def _validate(bundle: Path, tmp_path: Path) -> dict:
    return _validate_and_extract(bundle, tmp_path / "extracted", _sources())


def test_bundle_package_and_validation_round_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle_path = _prepare_bundle(tmp_path, monkeypatch)

    bundle = _validate(bundle_path, tmp_path)

    assert bundle["source_run_id"] == RUN_ID
    assert bundle["source_commit"] == COMMIT
    for name in SOURCE_NAMES:
        snapshot = tmp_path / "extracted" / name / "data.parquet"
        assert snapshot.is_file()
        assert pq.ParquetFile(snapshot).metadata.num_rows == 1


def test_packager_rejects_snapshot_changed_after_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SNAPSHOT_EXTRACTION_STARTED_AT", STARTED_AT)
    monkeypatch.setenv("GITHUB_RUN_ID", RUN_ID)
    monkeypatch.setenv("SNAPSHOT_SOURCE_COMMIT", COMMIT)
    bundle_path = _prepare_bundle(tmp_path, monkeypatch)
    snapshot = Path(
        "data/raw/active_permits/ingest_date=2026-10-26/run_id=synthetic/data.parquet"
    )
    snapshot.write_bytes(b"corrupted parquet")

    with pytest.raises(ValueError, match="SHA-256"):
        package_bundle(tmp_path / "invalid.zip")

    assert not (tmp_path / "invalid.zip").exists()
    assert bundle_path.exists()


def test_validator_rejects_wrong_selected_run_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _prepare_bundle(tmp_path, monkeypatch)
    monkeypatch.setenv("SNAPSHOT_SOURCE_RUN_ID", "999999999")

    with pytest.raises(ValueError, match="run ID"):
        _validate(bundle, tmp_path)


def test_validator_rejects_modified_snapshot_checksum(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _prepare_bundle(tmp_path, monkeypatch)
    bad_bundle = _rewrite_bundle(
        bundle,
        tmp_path / "modified.zip",
        replacements={"active_permits/data.parquet": b"corrupted snapshot"},
    )

    with pytest.raises(ValueError, match="SHA-256"):
        _validate(bad_bundle, tmp_path)


def test_validator_rejects_missing_bundle_member(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _prepare_bundle(tmp_path, monkeypatch)
    bad_bundle = _rewrite_bundle(
        bundle,
        tmp_path / "missing-member.zip",
        remove={"cleared_permits/manifest.json"},
    )

    with pytest.raises(ValueError, match="contents differ"):
        _validate(bad_bundle, tmp_path)


def test_validator_rejects_manifest_row_count_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _prepare_bundle(tmp_path, monkeypatch)
    with ZipFile(bundle) as archive:
        manifest = json.loads(archive.read("active_permits/manifest.json"))
    manifest["row_count"] = 2
    bad_bundle = _rewrite_bundle(
        bundle,
        tmp_path / "wrong-count.zip",
        replacements={
            "active_permits/manifest.json": json.dumps(manifest).encode("utf-8")
        },
    )

    with pytest.raises(ValueError, match="row counts disagree"):
        _validate(bad_bundle, tmp_path)


def test_validator_rejects_missing_required_source_column(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _prepare_bundle(
        tmp_path, monkeypatch, omit_columns={"active_permits": {"STATUS"}}
    )

    with pytest.raises(ValueError, match="missing required columns"):
        _validate(bundle, tmp_path)


def test_validator_rejects_failed_load_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _prepare_bundle(tmp_path, monkeypatch)
    with ZipFile(bundle) as archive:
        manifest = json.loads(archive.read("active_permits/manifest.json"))
    manifest["status"] = "load_failed"
    bad_bundle = _rewrite_bundle(
        bundle,
        tmp_path / "failed-load.zip",
        replacements={
            "active_permits/manifest.json": json.dumps(manifest).encode("utf-8")
        },
    )

    with pytest.raises(ValueError, match="not a successful load"):
        _validate(bad_bundle, tmp_path)
