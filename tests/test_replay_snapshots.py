from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import replay_snapshots


def test_report_ledger_entries_joins_provenance_and_native_evidence(monkeypatch):
    provenance = {
        "snapshot_id": "123:1",
        "source_name": "active_permits",
        "status": "succeeded",
        "run_id": "123",
    }
    native = {
        "snapshot_id": "123:1",
        "source_name": "active_permits",
        "status": "succeeded",
        "quarantined_row_count": 2,
        "quarantined_bad_row_count": 1,
        "quarantined_business_key_count": 1,
        "quarantine_sha256": "a" * 64,
    }
    target = object()
    observed = []

    monkeypatch.setattr(
        replay_snapshots.replay_warehouse_ledger,
        "snapshot_records",
        lambda value: observed.append(value) or [native],
    )

    entries = replay_snapshots._report_ledger_entries(
        SimpleNamespace(snapshot_records=lambda backend: [provenance]),
        target,
        "duckdb",
    )

    assert observed == [target]
    assert entries == [
        {
            **provenance,
            "warehouse_status": "succeeded",
            "quarantined_row_count": 2,
            "quarantined_bad_row_count": 1,
            "quarantined_business_key_count": 1,
            "quarantine_sha256": "a" * 64,
        }
    ]


def test_report_ledger_entries_rejects_missing_native_evidence(monkeypatch):
    monkeypatch.setattr(
        replay_snapshots.replay_warehouse_ledger,
        "snapshot_records",
        lambda target: [],
    )
    ledger = SimpleNamespace(
        snapshot_records=lambda backend: [
            {"snapshot_id": "123:1", "source_name": "active_permits"}
        ]
    )

    with pytest.raises(RuntimeError, match="Candidate ledger is missing"):
        replay_snapshots._report_ledger_entries(ledger, object(), "duckdb")


def test_report_ledger_entries_skips_native_read_without_provenance(monkeypatch):
    monkeypatch.setattr(
        replay_snapshots.replay_warehouse_ledger,
        "snapshot_records",
        lambda target: pytest.fail("Native ledger should not be queried."),
    )
    ledger = SimpleNamespace(snapshot_records=lambda backend: [])

    assert replay_snapshots._report_ledger_entries(ledger, object(), "duckdb") == []


def test_main_reuses_one_local_generation_id_for_run_and_finalization(
    tmp_path, monkeypatch
):
    from scripts import replay_ledger, validate_snapshot_replay

    ledger_path = tmp_path / "local-generation.sqlite"
    ledger = replay_ledger.ReplayLedger(ledger_path)
    args = SimpleNamespace(
        mode="rebuild",
        backend="duckdb",
        generation_id=None,
        ledger=ledger_path,
    )

    generated_ids = iter(("local:first", "local:second"))

    def generation_id(parsed_args):
        explicit = getattr(parsed_args, "generation_id", None)
        return str(explicit) if explicit else next(generated_ids)

    def fake_run(parsed_args):
        current_id = replay_snapshots._generation_id(parsed_args)
        ledger.start_generation(
            "duckdb",
            current_id,
            mode="rebuild",
            history_scope="full",
            selected_snapshot_count=1,
            candidate="candidate.duckdb",
        )
        return {"status": "passed"}

    monkeypatch.setattr(
        replay_snapshots,
        "_parser",
        lambda: SimpleNamespace(parse_args=lambda: args),
    )
    monkeypatch.setattr(replay_snapshots, "_generation_id", generation_id)
    monkeypatch.setattr(replay_snapshots, "run", fake_run)
    monkeypatch.setattr(
        validate_snapshot_replay,
        "validate_backend_report",
        lambda report, backend: None,
    )

    assert replay_snapshots.main() == 0
    assert ledger.generation_record("duckdb", "local:first")["status"] == "SUCCEEDED"
    assert ledger.generation_record("duckdb", "local:second") is None


@pytest.mark.parametrize(
    ("validation_error", "expected_status", "expected_exit"),
    [
        (None, "SUCCEEDED", 0),
        ("SCD2 validation failed", "FAILED", 1),
    ],
)
def test_main_finalizes_generation_after_backend_validation(
    tmp_path, monkeypatch, validation_error, expected_status, expected_exit
):
    from scripts import replay_ledger, validate_snapshot_replay

    ledger_path = tmp_path / "generation.sqlite"
    generation_id = "test-run:1"
    ledger = replay_ledger.ReplayLedger(ledger_path)
    ledger.start_generation(
        "duckdb",
        generation_id,
        mode="rebuild",
        history_scope="full",
        selected_snapshot_count=1,
        candidate="candidate.duckdb",
    )

    args = SimpleNamespace(
        mode="rebuild",
        backend="duckdb",
        generation_id=generation_id,
        ledger=ledger_path,
    )
    monkeypatch.setattr(
        replay_snapshots,
        "_parser",
        lambda: SimpleNamespace(parse_args=lambda: args),
    )
    monkeypatch.setattr(replay_snapshots, "run", lambda _args: {"status": "passed"})

    def validate(report, backend):
        assert backend == "duckdb"
        assert ledger.generation_record("duckdb", generation_id)["status"] == "RUNNING"
        if validation_error:
            raise ValueError(validation_error)

    monkeypatch.setattr(validate_snapshot_replay, "validate_backend_report", validate)

    assert replay_snapshots.main() == expected_exit
    record = ledger.generation_record("duckdb", generation_id)
    assert record["status"] == expected_status
    if validation_error:
        assert validation_error in record["error"]


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
    assert args.database is None
    assert args.schema is None


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


def test_run_uses_latest_successful_archive_as_quality_baseline_before_target_open(
    tmp_path, monkeypatch
):
    import json
    from datetime import UTC, datetime

    seen = {"steps": []}

    def quality_archive(snapshot_id, day, run_id, archive_sha256):
        extracted_at = datetime.fromisoformat(day).replace(tzinfo=UTC)
        source_records = {}
        sources = {}

        for source_name in ("active_permits", "cleared_permits"):
            sources[source_name] = SimpleNamespace(
                extracted_at=extracted_at,
                row_count=100,
                error=None,
                manifest={"quality_warnings": []},
            )
            source_records[source_name] = {
                "status": "succeeded",
                "order_timestamp": round(extracted_at.timestamp(), 6),
            }

        archive = SimpleNamespace(
            snapshot_id=snapshot_id,
            extracted_at=extracted_at,
            order=(extracted_at, int(run_id), 1, snapshot_id),
            format_version=2,
            run_id=str(run_id),
            run_attempt=1,
            source_commit="a" * 40,
            archive_sha256=archive_sha256,
            release_tag="toronto-raw-snapshot-v2-archive-2026-11",
            asset_name=f"toronto-raw-snapshot-v2-bundle-{snapshot_id}.zip",
            sources=sources,
            bundle={},
        )
        return archive, source_records

    baseline_archive, baseline_sources = quality_archive(
        "baseline", "2026-11-10T00:00:00", 10, "b" * 64
    )
    pending_archive, _ = quality_archive("pending", "2026-11-11T00:00:00", 11, "c" * 64)

    baseline_record = {
        "snapshot_id": "baseline",
        "format_version": 2,
        "run_id": "10",
        "run_attempt": 1,
        "source_commit": "a" * 40,
        "archive_sha256": "b" * 64,
        "release_tag": "toronto-raw-snapshot-v2-archive-2026-11",
        "asset_name": "toronto-raw-snapshot-v2-bundle-baseline.zip",
        "source_records": baseline_sources,
    }

    local_ledger = SimpleNamespace(
        completed_snapshot_ids=lambda backend: {"baseline"},
        latest_successful_snapshot=lambda backend: baseline_record,
        snapshot_records=lambda backend: [],
    )

    class FakeTarget:
        def __init__(self, *args, **kwargs):
            seen["target_mode"] = kwargs["mode"]
            seen["steps"].append("target-open")

        def completed_snapshot_ids(self):
            return {"baseline"}

    def fake_validate_baseline(target, snapshot_id, snapshot_count):
        seen["steps"].append("baseline-preflight")
        seen["baseline"] = (snapshot_id, snapshot_count)
        return {
            "status": "passed",
            "baseline_snapshot_id": snapshot_id,
            "snapshot_count": snapshot_count,
        }

    def fake_discover(repository, destination, *, exclude_snapshot_ids=None):
        seen["excluded"] = set(exclude_snapshot_ids or ())
        seen["steps"].append("discovery")
        return [pending_archive, baseline_archive]

    def fake_quality_gate(archives):
        seen["steps"].append("quality")
        seen["quality_ids"] = [item.snapshot_id for item in archives]

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
    monkeypatch.setattr(
        replay_snapshots, "validate_warehouse_eligibility", fake_quality_gate
    )
    monkeypatch.setattr(replay_snapshots, "replay_backend", fake_replay_backend)
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "github-output.txt"))
    monkeypatch.setattr(
        replay_snapshots.replay_warehouse_ledger,
        "validate_canonical_baseline",
        fake_validate_baseline,
    )

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
            "--baseline-snapshot-id",
            "42022028625:1",
            "--baseline-snapshot-count",
            "28",
        ]
    )
    report = replay_snapshots.run(args)

    assert seen["steps"] == [
        "discovery",
        "quality",
        "target-open",
        "baseline-preflight",
    ]
    assert seen["target_mode"] == "incremental"
    assert seen["baseline"] == ("42022028625:1", 28)
    assert seen["excluded"] == set()
    assert seen["quality_ids"] == ["baseline", "pending"]
    assert seen["loaded_ids"] == ["pending"]
    assert report["warehouse_quality_preflight"]["status"] == "passed"
    assert report["warehouse_quality_preflight"]["baseline_snapshot_id"] == "baseline"
    assert report["snapshot_count_discovered"] == 1
    assert report["history_scope"] == "incremental"
    assert report["explicit_promotion_required"] is True

    saved = json.loads((tmp_path / "summary.json").read_text())
    assert saved["warehouse_quality_preflight"]["baseline_snapshot_id"] == "baseline"
    outputs = (tmp_path / "github-output.txt").read_text()
    assert "has_pending_archives=true" in outputs
    assert "history_scope=incremental" in outputs


def test_incremental_quality_gate_rejects_missing_baseline_archive_before_target_open(
    tmp_path, monkeypatch
):
    local_ledger = SimpleNamespace(
        completed_snapshot_ids=lambda backend: {"baseline"},
        latest_successful_snapshot=lambda backend: {"snapshot_id": "baseline"},
        snapshot_records=lambda backend: [],
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("Candidate/config was opened before baseline validation.")

    monkeypatch.setattr(replay_snapshots, "ReplayLedger", lambda path: local_ledger)
    monkeypatch.setattr(
        replay_snapshots,
        "discover_archives",
        lambda repository, destination, *, exclude_snapshot_ids=None: [
            _archive("pending", "2026-11-11T00:00:00", 11)
        ],
    )
    monkeypatch.setattr(replay_snapshots, "_source_configs", forbidden)
    monkeypatch.setattr(replay_snapshots, "DuckDBReplayTarget", forbidden)
    monkeypatch.setattr(
        replay_snapshots,
        "validate_warehouse_eligibility",
        forbidden,
    )

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
            "--baseline-snapshot-id",
            "42022028625:1",
            "--baseline-snapshot-count",
            "28",
        ]
    )

    with pytest.raises(RuntimeError, match="baseline archive.*present exactly once"):
        replay_snapshots.run(args)


def test_rebuild_discovers_all_archives_before_selecting_scope(tmp_path, monkeypatch):
    generation_started = {}
    local_ledger = SimpleNamespace(
        completed_snapshot_ids=lambda backend: {"completed"},
        snapshot_records=lambda backend: [],
        start_generation=lambda backend, generation_id, **details: (
            generation_started.update(
                backend=backend, generation_id=generation_id, **details
            )
        ),
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
    monkeypatch.setattr(
        replay_snapshots,
        "_validate_full_history_inventory",
        lambda args: fake_discover(
            args.repository, args.archive_dir, exclude_snapshot_ids=set()
        ),
    )
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
            "--database",
            str(tmp_path / "candidate.duckdb"),
            "--limit",
            "2",
        ]
    )
    report = replay_snapshots.run(args)

    assert seen["excluded"] == set()
    assert seen["selected"] == ["pending", "later"]
    assert report["history_scope"] == "partial"
    assert generation_started["backend"] == "duckdb"
    assert generation_started["history_scope"] == "partial"
    assert generation_started["selected_snapshot_count"] == 2
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


def test_rebuild_requires_explicit_backend_candidate_target(tmp_path, monkeypatch):
    import pytest

    def forbidden(*args, **kwargs):
        raise AssertionError("Replay reached external work without a candidate target.")

    monkeypatch.setattr(replay_snapshots, "_validate_full_history_inventory", forbidden)
    monkeypatch.setattr(replay_snapshots, "_source_configs", forbidden)
    monkeypatch.setattr(replay_snapshots, "ReplayLedger", forbidden)
    monkeypatch.setattr(replay_snapshots, "_build_target", forbidden)

    cases = (
        ("duckdb", "DuckDB rebuild requires an explicit --database"),
        ("snowflake", "Snowflake rebuild requires an explicit --schema"),
    )
    for backend, message in cases:
        args = replay_snapshots._parser().parse_args(
            [
                "--mode",
                "rebuild",
                "--backend",
                backend,
                "--ledger",
                str(tmp_path / f"{backend}-ledger.sqlite"),
                "--output",
                str(tmp_path / f"{backend}-summary.json"),
            ]
        )
        with pytest.raises(ValueError, match=message):
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


def test_incremental_requires_baseline_before_opening_target(tmp_path, monkeypatch):
    import pytest

    def unexpected(*args, **kwargs):
        raise AssertionError("Replay proceeded without baseline parameters.")

    monkeypatch.setattr(replay_snapshots, "_source_configs", unexpected)
    monkeypatch.setattr(replay_snapshots, "ReplayLedger", unexpected)
    monkeypatch.setattr(replay_snapshots, "_build_target", unexpected)

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
        ]
    )

    with pytest.raises(ValueError, match="baseline-snapshot-id"):
        replay_snapshots.run(args)


def test_rebuild_inventory_gate_runs_before_candidate_open(tmp_path, monkeypatch):
    import pytest

    def forbidden(*args, **kwargs):
        raise AssertionError("candidate/config/ledger opened before inventory check")

    def reject_inventory(args):
        raise ValueError("inventory does not match Release assets")

    monkeypatch.setattr(
        replay_snapshots, "_validate_full_history_inventory", reject_inventory
    )
    monkeypatch.setattr(replay_snapshots, "_source_configs", forbidden)
    monkeypatch.setattr(replay_snapshots, "ReplayLedger", forbidden)
    monkeypatch.setattr(replay_snapshots, "_build_target", forbidden)

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
            "--database",
            str(tmp_path / "candidate.duckdb"),
        ]
    )
    with pytest.raises(ValueError, match="inventory does not match"):
        replay_snapshots.run(args)
