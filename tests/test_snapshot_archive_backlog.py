from __future__ import annotations

import json
from datetime import UTC, datetime
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from scripts.snapshot_archive_backlog import (
    _flatten_pages,
    _validate_archive_metadata,
    snapshot_order,
)


def _archive(
    path,
    *,
    tag="toronto-raw-snapshot-archive-2026-11",
    complete=True,
    run_id="123456",
    attempt="2",
):
    bundle = {
        "format_version": 1,
        "snapshot_stage": "raw",
        "source_run_id": run_id,
        "source_run_attempt": attempt,
        "source_commit": "a" * 40,
        "extraction_started_at_utc": "2026-11-07T15:00:00+00:00",
        "complete": complete,
        "approved_for_warehouse": complete,
        "sources": {
            "active_permits": {"warehouse_eligible": complete},
            "cleared_permits": {"warehouse_eligible": complete},
        },
    }
    asset = f"toronto-raw-snapshot-bundle-{run_id}-{attempt}.zip"
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("bundle.json", json.dumps(bundle))
    return tag, asset


def test_snapshot_order_uses_timestamp_then_numeric_run_and_attempt():
    earlier = snapshot_order("2026-11-07T15:00:00Z", "99", "9")
    later_time = snapshot_order("2026-11-07T15:00:01Z", "1", "1")
    later_attempt = snapshot_order("2026-11-07T15:00:00Z", "100", "2")
    later_run = snapshot_order("2026-11-07T15:00:00Z", "100", "1")

    assert earlier < later_time
    assert later_run < later_attempt
    assert (
        snapshot_order(datetime(2026, 11, 7, 15, 0, tzinfo=UTC), "100", "1")
        == later_run
    )


def test_snapshot_order_rejects_naive_timestamp_and_non_numeric_identity():
    with pytest.raises(ValueError, match="timezone"):
        snapshot_order(datetime(2026, 11, 7, 15, 0), "100", "1")
    with pytest.raises(ValueError, match="digits"):
        snapshot_order("2026-11-07T15:00:00Z", "run-100", "1")


def test_approved_monthly_archive_metadata_is_accepted(tmp_path):
    path = tmp_path / "archive.zip"
    tag, asset = _archive(path)

    result = _validate_archive_metadata(path, tag, asset)

    assert result is not None
    assert result.snapshot_id == "123456:2"
    assert result.order[0] == datetime(2026, 11, 7, 15, 0, tzinfo=UTC)


def test_incomplete_archive_is_preserved_but_not_replayed(tmp_path):
    path = tmp_path / "partial.zip"
    tag, asset = _archive(path, complete=False)

    assert _validate_archive_metadata(path, tag, asset) is None


def test_archive_release_month_must_match_utc_extraction_month(tmp_path):
    path = tmp_path / "wrong-month.zip"
    _, asset = _archive(path)

    with pytest.raises(ValueError, match="UTC extraction timestamp"):
        _validate_archive_metadata(
            path,
            "toronto-raw-snapshot-archive-2026-10",
            asset,
        )


def test_flatten_pages_handles_paginated_and_slurped_arrays():
    pages = [[{"id": 1}, {"id": 2}], [{"id": 3}]]
    assert _flatten_pages(pages) == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_bundle_claiming_approval_must_include_both_eligible_sources(tmp_path):
    path = tmp_path / "inconsistent.zip"
    tag, asset = _archive(path)
    with ZipFile(path, "r") as source:
        bundle = json.loads(source.read("bundle.json"))
    bundle["sources"].pop("cleared_permits")
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as output:
        output.writestr("bundle.json", json.dumps(bundle))

    with pytest.raises(ValueError, match="declares itself complete and approved"):
        _validate_archive_metadata(path, tag, asset)


def test_bundle_claiming_approval_must_not_contain_quality_blockers(tmp_path):
    path = tmp_path / "blocked-but-approved.zip"
    tag, asset = _archive(path)
    with ZipFile(path, "r") as source:
        bundle = json.loads(source.read("bundle.json"))
    bundle["sources"]["active_permits"]["warehouse_quality_blockers"] = [
        "row_count_anomaly"
    ]
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as output:
        output.writestr("bundle.json", json.dumps(bundle))

    with pytest.raises(ValueError, match="quality blockers"):
        _validate_archive_metadata(path, tag, asset)


def test_manual_snowflake_replay_includes_every_earlier_approved_archive():
    from types import SimpleNamespace

    from scripts.replay_snowflake_snapshot_backlog import _archives_through_selection

    archives = [
        SimpleNamespace(run_id="100", attempt="1"),
        SimpleNamespace(run_id="101", attempt="2"),
        SimpleNamespace(run_id="102", attempt="1"),
    ]
    result = _archives_through_selection(archives, run_id="101", attempt="2")
    assert [(item.run_id, item.attempt) for item in result] == [
        ("100", "1"),
        ("101", "2"),
    ]


def test_manual_snowflake_replay_rejects_target_not_in_backlog():
    from scripts.replay_snowflake_snapshot_backlog import _archives_through_selection

    with pytest.raises(ValueError, match="missing or not approved"):
        _archives_through_selection([], run_id="100", attempt="1")


def test_pending_snowflake_backlog_filters_only_fully_completed_ids():
    from types import SimpleNamespace

    from scripts.replay_snowflake_snapshot_backlog import _pending_archives

    archives = [
        SimpleNamespace(snapshot_id="100:1"),
        SimpleNamespace(snapshot_id="101:1"),
        SimpleNamespace(snapshot_id="102:2"),
    ]
    result = _pending_archives(archives, {"100:1", "102:2"})
    assert [item.snapshot_id for item in result] == ["101:1"]


def test_discovery_skips_fully_completed_assets_before_downloading(
    tmp_path, monkeypatch
):
    from scripts import snapshot_archive_backlog as backlog

    assets = [
        (
            "toronto-raw-snapshot-archive-2026-11",
            "toronto-raw-snapshot-bundle-123456-2.zip",
            1,
        ),
        (
            "toronto-raw-snapshot-archive-2026-11",
            "toronto-raw-snapshot-bundle-123457-1.zip",
            1,
        ),
    ]
    monkeypatch.setattr(backlog, "_monthly_release_assets", lambda repository: assets)
    downloaded = []

    def fake_download(repository, tag, asset_name, destination):
        downloaded.append(asset_name)
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / asset_name
        _archive(
            path,
            run_id="123457",
            attempt="1",
        )
        return path

    monkeypatch.setattr(backlog, "_download_asset", fake_download)

    found = backlog.discover_archives(
        "gagadanalyst/toronto-housing-bottleneck-analytics",
        tmp_path,
        exclude_snapshot_ids={"123456:2"},
    )

    assert downloaded == ["toronto-raw-snapshot-bundle-123457-1.zip"]
    assert [item.snapshot_id for item in found] == ["123457:1"]


def test_manual_selection_can_force_download_of_completed_archive(
    tmp_path, monkeypatch
):
    from scripts import snapshot_archive_backlog as backlog

    asset = "toronto-raw-snapshot-bundle-123456-2.zip"
    monkeypatch.setattr(
        backlog,
        "_monthly_release_assets",
        lambda repository: [("toronto-raw-snapshot-archive-2026-11", asset, 1)],
    )
    calls = []

    def fake_download(repository, tag, asset_name, destination):
        calls.append(asset_name)
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / asset_name
        _archive(path)
        return path

    monkeypatch.setattr(backlog, "_download_asset", fake_download)

    found = backlog.discover_archives(
        "gagadanalyst/toronto-housing-bottleneck-analytics",
        tmp_path,
        exclude_snapshot_ids={"123456:2"},
        include_snapshot_ids={"123456:2"},
    )

    assert calls == [asset]
    assert [item.snapshot_id for item in found] == ["123456:2"]


def test_discovery_filters_by_run_ids_before_downloading(tmp_path, monkeypatch):
    from scripts import snapshot_archive_backlog as backlog

    assets = [
        (
            "toronto-raw-snapshot-archive-2026-11",
            "toronto-raw-snapshot-bundle-123456-2.zip",
            1,
        ),
        (
            "toronto-raw-snapshot-archive-2026-11",
            "toronto-raw-snapshot-bundle-123457-1.zip",
            1,
        ),
    ]
    monkeypatch.setattr(backlog, "_monthly_release_assets", lambda repository: assets)
    downloaded = []

    def fake_download(repository, tag, asset_name, destination):
        downloaded.append(asset_name)
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / asset_name
        _archive(path, run_id="123457", attempt="1")
        return path

    monkeypatch.setattr(backlog, "_download_asset", fake_download)

    found = backlog.discover_archives(
        "gagadanalyst/toronto-housing-bottleneck-analytics",
        tmp_path,
        include_run_ids={"123457"},
    )

    assert downloaded == ["toronto-raw-snapshot-bundle-123457-1.zip"]
    assert [item.snapshot_id for item in found] == ["123457:1"]


def test_discovery_rejects_invalid_explicit_run_id_filter(tmp_path, monkeypatch):
    from scripts import snapshot_archive_backlog as backlog

    monkeypatch.setattr(backlog, "_monthly_release_assets", lambda repository: [])
    with pytest.raises(ValueError, match="non-empty numeric IDs"):
        backlog.discover_archives(
            "gagadanalyst/toronto-housing-bottleneck-analytics",
            tmp_path,
            include_run_ids={"run-123"},
        )
