from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from scripts import replay_snapshots


def test_asset_names_normalize_v1_and_v2_identity():
    assert (
        replay_snapshots._asset_snapshot_id("toronto-raw-snapshot-bundle-123-2.zip")
        == "123:2"
    )
    assert (
        replay_snapshots._asset_snapshot_id(
            "toronto-raw-snapshot-v2-bundle-ed44fa3159ec485ba73fa70cfae938b8.zip"
        )
        == "ed44fa3159ec485ba73fa70cfae938b8"
    )
    assert replay_snapshots._asset_snapshot_id("unrelated.zip") is None


def test_discovery_filters_completed_assets_before_download_and_sorts(
    tmp_path, monkeypatch
):
    releases = [
        {"id": 1, "tag_name": "toronto-raw-snapshot-archive-2026-11"},
        {"id": 2, "tag_name": "toronto-raw-snapshot-v2-archive-2026-11"},
        {"id": 3, "tag_name": "draft", "draft": True},
    ]
    assets = {
        1: [
            {"name": "toronto-raw-snapshot-bundle-123-1.zip", "size": 100},
            {"name": "toronto-raw-snapshot-bundle-124-1.zip", "size": 100},
        ],
        2: [
            {
                "name": (
                    "toronto-raw-snapshot-v2-bundle-"
                    "ed44fa3159ec485ba73fa70cfae938b8.zip"
                ),
                "size": 100,
            }
        ],
    }

    def fake_gh_json(endpoint):
        if endpoint.endswith("releases?per_page=100"):
            return [releases]
        release_id = int(endpoint.split("/releases/")[1].split("/")[0])
        return [assets[release_id]]

    downloaded = []

    def fake_run(command, **kwargs):
        if command[1] == "release":
            tag = command[3]
            name = command[command.index("--pattern") + 1]
            directory = Path(command[command.index("--dir") + 1])
            directory.mkdir(parents=True, exist_ok=True)
            (directory / name).write_bytes(b"archive")
            downloaded.append((tag, name))
        return SimpleNamespace(stdout="", stderr="", returncode=0)

    inspected = []

    def fake_inspect(path, tag, asset, repository):
        snapshot_id = replay_snapshots._asset_snapshot_id(asset)
        order_time = {"123:1": 1, "124:1": 2}.get(snapshot_id, 0)
        inspected.append(snapshot_id)
        return SimpleNamespace(
            snapshot_id=snapshot_id,
            order=(order_time, 123, 1, snapshot_id),
        )

    monkeypatch.setattr(replay_snapshots, "_gh_json", fake_gh_json)
    monkeypatch.setattr(replay_snapshots.subprocess, "run", fake_run)
    monkeypatch.setattr(replay_snapshots, "inspect_archive", fake_inspect)

    found = replay_snapshots.discover_archives(
        "owner/repo",
        tmp_path,
        exclude_snapshot_ids={"123:1"},
    )

    assert inspected == ["124:1", "ed44fa3159ec485ba73fa70cfae938b8"]
    assert [item.snapshot_id for item in found] == [
        "ed44fa3159ec485ba73fa70cfae938b8",
        "124:1",
    ]
    assert all("123-1.zip" not in name for _, name in downloaded)


def test_discovery_rejects_invalid_release_asset_size(tmp_path, monkeypatch):
    monkeypatch.setattr(
        replay_snapshots,
        "_gh_json",
        lambda endpoint: (
            [[{"id": 1, "tag_name": "toronto-raw-snapshot-archive-2026-11"}]]
            if endpoint.endswith("releases?per_page=100")
            else [[{"name": "toronto-raw-snapshot-bundle-123-1.zip", "size": 0}]]
        ),
    )
    try:
        replay_snapshots.discover_archives("owner/repo", tmp_path)
    except ValueError as exc:
        assert "invalid size" in str(exc)
    else:
        raise AssertionError("invalid archive size was accepted")


def _archive(snapshot_id: str, day: str, run_id: int):
    from datetime import UTC, datetime

    return SimpleNamespace(
        snapshot_id=snapshot_id,
        extracted_at=datetime.fromisoformat(day).replace(tzinfo=UTC),
        order=(datetime.fromisoformat(day).replace(tzinfo=UTC), run_id, 1, snapshot_id),
        format_version=2,
        source_commit="a" * 40,
        run_id=str(run_id),
        run_attempt=1,
        release_tag="toronto-raw-snapshot-v2-archive-2026-11",
        asset_name=f"toronto-raw-snapshot-v2-bundle-{snapshot_id}.zip",
        archive_sha256="b" * 64,
    )


def test_parser_accepts_both_modes_and_selection_arguments():
    args = replay_snapshots._parser().parse_args(
        [
            "--mode",
            "rebuild",
            "--backend",
            "duckdb",
            "--ledger",
            "ledger.sqlite",
            "--output",
            "summary.json",
            "--from-date",
            "2026-11-02",
            "--to-date",
            "2026-11-08",
            "--last-n",
            "3",
        ]
    )
    assert args.mode == "rebuild"
    assert args.from_date == "2026-11-02"
    assert args.to_date == "2026-11-08"
    assert args.limit == 3


def test_parser_rejects_nonpositive_limit():
    import pytest

    with pytest.raises(SystemExit):
        replay_snapshots._parser().parse_args(
            [
                "--backend",
                "duckdb",
                "--ledger",
                "ledger.sqlite",
                "--output",
                "summary.json",
                "--limit",
                "0",
            ]
        )


def test_rebuild_selection_is_chronological_and_uses_inclusive_utc_dates():
    archives = [
        _archive("late", "2026-11-10T00:00:00", 3),
        _archive("middle", "2026-11-05T00:00:00", 2),
        _archive("early", "2026-11-01T00:00:00", 1),
    ]
    selected, scope = replay_snapshots.select_archives(
        archives,
        mode="rebuild",
        from_date="2026-11-01",
        to_date="2026-11-10",
        limit=2,
    )
    assert scope == "partial"
    assert [item.snapshot_id for item in selected] == ["middle", "late"]


def test_unfiltered_rebuild_is_full_history():
    selected, scope = replay_snapshots.select_archives(
        [_archive("one", "2026-11-01T00:00:00", 1)],
        mode="rebuild",
    )
    assert scope == "full"
    assert len(selected) == 1


def test_incremental_rejects_selection_and_allows_empty_backlog():
    import pytest

    selected, scope = replay_snapshots.select_archives([], mode="incremental")
    assert selected == []
    assert scope == "incremental"
    with pytest.raises(ValueError, match="only in rebuild mode"):
        replay_snapshots.select_archives([], mode="incremental", limit=2)


def test_rebuild_last_n_is_applied_before_retry_completion_skips():
    # Selection receives the complete discovered archive list. A retry can then
    # skip already-loaded snapshots without expanding the requested N-snapshot set.
    archives = [
        _archive("one", "2026-11-01T00:00:00", 1),
        _archive("two", "2026-11-02T00:00:00", 2),
        _archive("three", "2026-11-03T00:00:00", 3),
        _archive("four", "2026-11-04T00:00:00", 4),
    ]
    selected, scope = replay_snapshots.select_archives(
        archives,
        mode="rebuild",
        limit=3,
    )
    completed_from_previous_attempt = {"one", "two"}
    pending = [
        item
        for item in selected
        if item.snapshot_id not in completed_from_previous_attempt
    ]
    assert scope == "partial"
    assert [item.snapshot_id for item in selected] == ["two", "three", "four"]
    assert [item.snapshot_id for item in pending] == ["three", "four"]


def test_run_passes_persistent_completed_ids_to_incremental_discovery(
    tmp_path, monkeypatch
):
    import json

    local_ledger = SimpleNamespace(
        completed_snapshot_ids=lambda backend: {"local-complete"},
        snapshot_records=lambda backend: [],
    )
    seen = {}

    class FakeTarget:
        def __init__(self, *args, **kwargs):
            seen["target_mode"] = kwargs["mode"]

        def completed_snapshot_ids(self):
            return {"warehouse-complete"}

    def fake_discover(repository, destination, *, exclude_snapshot_ids=None):
        seen["excluded"] = set(exclude_snapshot_ids or ())
        return [_archive("pending", "2026-11-10T00:00:00", 10)]

    def fake_replay_backend(archives, **kwargs):
        seen["loaded_ids"] = [item.snapshot_id for item in archives]
        return {
            "backend": "duckdb",
            "status": "passed",
            "sources": {},
            "history_validation": {"status": "passed", "sources": {}},
        }

    monkeypatch.setattr(
        replay_snapshots,
        "_source_configs",
        lambda path: {"active_permits": object(), "cleared_permits": object()},
    )
    monkeypatch.setattr(replay_snapshots, "ReplayLedger", lambda path: local_ledger)
    monkeypatch.setattr(replay_snapshots, "DuckDBReplayTarget", FakeTarget)
    monkeypatch.setattr(replay_snapshots, "discover_archives", fake_discover)
    monkeypatch.setattr(replay_snapshots, "replay_backend", fake_replay_backend)
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "github-output.txt"))

    args = replay_snapshots._parser().parse_args(
        [
            "--mode",
            "incremental",
            "--backend",
            "duckdb",
            "--ledger",
            str(tmp_path / "ledger.sqlite"),
            "--output",
            str(tmp_path / "summary.json"),
            "--database",
            "data/staging/duckdb/candidate-10-1.duckdb",
        ]
    )
    report = replay_snapshots.run(args)

    assert seen["target_mode"] == "incremental"
    assert seen["excluded"] == {"local-complete", "warehouse-complete"}
    assert seen["loaded_ids"] == ["pending"]
    assert report["history_scope"] == "incremental"
    assert report["explicit_promotion_required"] is True
    saved = json.loads((tmp_path / "summary.json").read_text())
    assert saved["mode"] == "incremental"
    outputs = (tmp_path / "github-output.txt").read_text()
    assert "has_pending_archives=true" in outputs
    assert "history_scope=incremental" in outputs


def test_rebuild_discovers_all_archives_before_selecting_scope(tmp_path, monkeypatch):
    local_ledger = SimpleNamespace(
        completed_snapshot_ids=lambda backend: {"completed"},
        snapshot_records=lambda backend: [],
    )
    target = SimpleNamespace(completed_snapshot_ids=lambda: {"completed"})
    discovered = [
        _archive("completed", "2026-11-01T00:00:00", 1),
        _archive("pending", "2026-11-02T00:00:00", 2),
        _archive("later", "2026-11-03T00:00:00", 3),
    ]
    seen = {}

    def fake_discover(repository, destination, *, exclude_snapshot_ids=None):
        seen["excluded"] = set(exclude_snapshot_ids or ())
        return discovered

    def fake_replay_backend(archives, **kwargs):
        seen["selected"] = [item.snapshot_id for item in archives]
        return {
            "backend": "duckdb",
            "status": "passed",
            "sources": {},
            "history_validation": {"status": "passed", "sources": {}},
        }

    monkeypatch.setattr(
        replay_snapshots,
        "_source_configs",
        lambda path: {"active_permits": object(), "cleared_permits": object()},
    )
    monkeypatch.setattr(replay_snapshots, "ReplayLedger", lambda path: local_ledger)
    monkeypatch.setattr(replay_snapshots, "_build_target", lambda args: target)
    monkeypatch.setattr(replay_snapshots, "discover_archives", fake_discover)
    monkeypatch.setattr(replay_snapshots, "replay_backend", fake_replay_backend)

    args = replay_snapshots._parser().parse_args(
        [
            "--mode",
            "rebuild",
            "--backend",
            "duckdb",
            "--ledger",
            str(tmp_path / "ledger.sqlite"),
            "--output",
            str(tmp_path / "summary.json"),
            "--limit",
            "2",
        ]
    )
    report = replay_snapshots.run(args)

    assert seen["excluded"] == set()
    assert seen["selected"] == ["pending", "later"]
    assert report["history_scope"] == "partial"
    assert report["snapshot_count_selected"] == 2


def test_invalid_rebuild_date_is_rejected_before_config_or_target_open(
    tmp_path, monkeypatch
):
    import pytest

    def should_not_run(*args, **kwargs):
        raise AssertionError("replay side effect occurred before scope validation")

    monkeypatch.setattr(replay_snapshots, "_source_configs", should_not_run)
    monkeypatch.setattr(replay_snapshots, "ReplayLedger", should_not_run)
    monkeypatch.setattr(replay_snapshots, "_build_target", should_not_run)
    args = replay_snapshots._parser().parse_args(
        [
            "--mode",
            "rebuild",
            "--backend",
            "duckdb",
            "--ledger",
            str(tmp_path / "ledger.sqlite"),
            "--output",
            str(tmp_path / "summary.json"),
            "--from-date",
            "2026-99-99",
        ]
    )
    with pytest.raises(ValueError, match="valid calendar date"):
        replay_snapshots.run(args)


def test_rebuild_last_n_selects_newest_archives_within_date_window():
    archives = [
        _archive("older", "2026-11-01T00:00:00", 1),
        _archive("in-window-one", "2026-11-03T00:00:00", 2),
        _archive("in-window-two", "2026-11-05T00:00:00", 3),
        _archive("newer-outside", "2026-11-10T00:00:00", 4),
    ]
    selected, scope = replay_snapshots.select_archives(
        archives,
        mode="rebuild",
        from_date="2026-11-02",
        to_date="2026-11-06",
        limit=1,
    )
    assert scope == "partial"
    assert [item.snapshot_id for item in selected] == ["in-window-two"]
