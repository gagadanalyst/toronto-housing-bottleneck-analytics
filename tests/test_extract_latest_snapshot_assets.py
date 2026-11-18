from __future__ import annotations

import hashlib
import json

from scripts import extract_latest_snapshot_assets as helper


def test_extract_latest_uses_latest_selected_archive_and_both_sources(
    tmp_path, monkeypatch
):
    archive_dir = tmp_path / "archives"
    folder = archive_dir / "archive-00001"
    folder.mkdir(parents=True)
    zip_path = folder / "bundle.zip"
    zip_path.write_bytes(b"validated archive")
    summary = tmp_path / "summary.json"
    summary.write_text(
        json.dumps(
            {
                "mode": "incremental",
                "history_scope": "incremental",
                "snapshots": [
                    {
                        "snapshot_id": "older",
                        "release_tag": "tag",
                        "asset_name": "old.zip",
                        "archive_sha256": "a",
                    },
                    {
                        "snapshot_id": "newer",
                        "release_tag": "tag",
                        "asset_name": "bundle.zip",
                        "archive_sha256": "b",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    class Archive:
        archive_sha256 = "b"
        sources = {
            "active_permits": type(
                "S",
                (),
                {
                    "parquet_size_bytes": 5,
                    "parquet_sha256": hashlib.sha256(b"12345").hexdigest(),
                },
            )(),
            "cleared_permits": type(
                "S",
                (),
                {
                    "parquet_size_bytes": 6,
                    "parquet_sha256": hashlib.sha256(b"123456").hexdigest(),
                },
            )(),
        }

    monkeypatch.setattr(helper, "inspect_archive", lambda *args, **kwargs: Archive())

    def fake_extract(_archive, source_name, destination):
        path = destination / f"{source_name}.parquet"
        path.write_bytes(b"12345" if source_name == "active_permits" else b"123456")
        return path

    monkeypatch.setattr(helper, "extract_source", fake_extract)
    out = tmp_path / "output"
    values = helper.extract_latest(summary, archive_dir, out, repository="owner/repo")
    assert set(values) == {"active_permits", "cleared_permits"}
    assert (out / "toronto-active-permits.parquet").read_bytes() == b"12345"
    assert (out / "toronto-cleared-permits.parquet").read_bytes() == b"123456"


def test_extract_latest_accepts_only_full_mode2_selection(tmp_path, monkeypatch):
    archive_dir = tmp_path / "archives"
    folder = archive_dir / "archive-00001"
    folder.mkdir(parents=True)
    (folder / "bundle.zip").write_bytes(b"validated archive")
    summary = tmp_path / "summary.json"
    summary.write_text(
        json.dumps(
            {
                "mode": "rebuild",
                "history_scope": "full",
                "full_history": True,
                "explicit_promotion_required": True,
                "warehouse_input_staging": "not_used",
                "snapshot_count_discovered": 1,
                "snapshot_count_selected": 1,
                "selection": {"from_date": None, "to_date": None, "limit": None},
                "snapshots": [
                    {
                        "snapshot_id": "newer",
                        "release_tag": "tag",
                        "asset_name": "bundle.zip",
                        "archive_sha256": "b",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    class Archive:
        archive_sha256 = "b"
        sources = {
            "active_permits": type(
                "S",
                (),
                {
                    "parquet_size_bytes": 5,
                    "parquet_sha256": hashlib.sha256(b"12345").hexdigest(),
                },
            )(),
            "cleared_permits": type(
                "S",
                (),
                {
                    "parquet_size_bytes": 6,
                    "parquet_sha256": hashlib.sha256(b"123456").hexdigest(),
                },
            )(),
        }

    monkeypatch.setattr(helper, "inspect_archive", lambda *args, **kwargs: Archive())

    def fake_extract(_archive, source_name, destination):
        path = destination / f"{source_name}.parquet"
        path.write_bytes(b"12345" if source_name == "active_permits" else b"123456")
        return path

    monkeypatch.setattr(helper, "extract_source", fake_extract)
    result = helper.extract_latest(
        summary, archive_dir, tmp_path / "out", repository="owner/repo"
    )
    assert set(result) == {"active_permits", "cleared_permits"}


def test_extract_latest_rejects_partial_rebuild_and_empty_selection(tmp_path):
    summary = tmp_path / "summary.json"
    summary.write_text(
        json.dumps(
            {
                "mode": "rebuild",
                "history_scope": "partial",
                "full_history": False,
                "explicit_promotion_required": True,
                "warehouse_input_staging": "not_used",
                "snapshot_count_discovered": 2,
                "snapshot_count_selected": 1,
                "selection": {"from_date": None, "to_date": None, "limit": 1},
                "snapshots": [],
            }
        )
    )
    try:
        helper.extract_latest(
            summary, tmp_path, tmp_path / "out", repository="owner/repo"
        )
    except ValueError as exc:
        assert "validated full-history rebuild" in str(exc) or "no selected" in str(exc)
    else:
        raise AssertionError("Partial Mode 2 asset extraction was accepted")
