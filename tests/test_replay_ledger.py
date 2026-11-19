from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from scripts.replay_ledger import SOURCE_NAMES, ReplayLedger


@dataclass
class FakeArchive:
    snapshot_id: str
    extracted_at: datetime
    run_id: str
    run_attempt: int
    format_version: int = 2
    source_commit: str = "a" * 40
    archive_sha256: str = "b" * 64
    release_tag: str = "release"
    asset_name: str = "archive.zip"
    sources: dict | None = None

    def __post_init__(self):
        if self.sources is None:
            self.sources = {
                name: SimpleNamespace(
                    extracted_at=self.extracted_at,
                    manifest={
                        "extraction_started_at_utc": self.extracted_at.isoformat(),
                        "extraction_completed_at_utc": (
                            self.extracted_at + timedelta(seconds=15)
                        ).isoformat(),
                    },
                )
                for name in SOURCE_NAMES
            }

    @property
    def order(self):
        return self.extracted_at, int(self.run_id), self.run_attempt, self.snapshot_id


def _archive(snapshot_id, offset):
    return FakeArchive(
        snapshot_id=snapshot_id,
        extracted_at=datetime(2026, 11, 1, tzinfo=UTC) + timedelta(minutes=offset),
        run_id=str(1000 + offset),
        run_attempt=1,
    )


def _succeed(ledger, backend, source, snapshot_id):
    ledger.assert_can_load(backend, source, snapshot_id)
    ledger.mark(backend, source, snapshot_id, "loading")
    ledger.mark(backend, source, snapshot_id, "succeeded")


def test_registers_two_independent_source_rows_for_each_backend(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    archive = _archive("snapshot-1", 1)
    ledger.register_archives("duckdb", [archive])
    ledger.register_archives("snowflake", [archive])

    assert ledger.status("duckdb", "active_permits", "snapshot-1") == "pending"
    assert ledger.status("duckdb", "cleared_permits", "snapshot-1") == "pending"
    assert ledger.status("snowflake", "active_permits", "snapshot-1") == "pending"
    assert ledger.status("snowflake", "cleared_permits", "snapshot-1") == "pending"


def test_a_source_failure_does_not_block_the_other_source(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    archives = [_archive("snapshot-1", 1), _archive("snapshot-2", 2)]
    ledger.register_archives("duckdb", archives)
    ledger.mark("duckdb", "active_permits", "snapshot-1", "loading")
    ledger.mark("duckdb", "active_permits", "snapshot-1", "failed", error="bad rows")

    _succeed(ledger, "duckdb", "cleared_permits", "snapshot-1")
    _succeed(ledger, "duckdb", "cleared_permits", "snapshot-2")

    with pytest.raises(RuntimeError, match="earlier incomplete"):
        ledger.assert_can_load("duckdb", "active_permits", "snapshot-2")
    assert ledger.status("duckdb", "cleared_permits", "snapshot-2") == "succeeded"


def test_backend_failure_does_not_block_other_backend(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    archive = _archive("snapshot-1", 1)
    ledger.register_archives("duckdb", [archive])
    ledger.register_archives("snowflake", [archive])
    ledger.mark("duckdb", "active_permits", "snapshot-1", "loading")
    ledger.mark("duckdb", "active_permits", "snapshot-1", "failed")

    _succeed(ledger, "snowflake", "active_permits", "snapshot-1")
    _succeed(ledger, "snowflake", "cleared_permits", "snapshot-1")

    assert ledger.completed_snapshot_ids("snowflake") == {"snapshot-1"}
    assert ledger.completed_snapshot_ids("duckdb") == set()


def test_retry_preserves_success_and_retries_failed_source(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    archive = _archive("snapshot-1", 1)
    ledger.register_archives("duckdb", [archive])
    _succeed(ledger, "duckdb", "active_permits", "snapshot-1")
    ledger.mark("duckdb", "cleared_permits", "snapshot-1", "loading")
    ledger.mark("duckdb", "cleared_permits", "snapshot-1", "failed", error="retry")

    ledger.register_archives("duckdb", [archive])

    assert ledger.status("duckdb", "active_permits", "snapshot-1") == "succeeded"
    assert ledger.status("duckdb", "cleared_permits", "snapshot-1") == "failed"
    ledger.assert_can_load("duckdb", "cleared_permits", "snapshot-1")


def _create_accepted_mode2_ledger(
    path,
    *,
    generation_id="mode2-run:1",
    history_scope="full",
    terminal_status="SUCCEEDED",
):
    ledger = ReplayLedger(path)
    updated = "2026-11-18T00:00:00+00:00"

    with sqlite3.connect(path) as connection:
        for index in range(2):
            snapshot_id = f"snapshot-{index + 1}"
            for source_name in ("active_permits", "cleared_permits"):
                connection.execute(
                    """INSERT INTO replay_loads (
                           backend, source_name, snapshot_id, format_version,
                           order_timestamp, run_id, run_attempt, source_commit,
                           archive_sha256, release_tag, asset_name, status,
                           error, updated_at_utc
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        "duckdb",
                        source_name,
                        snapshot_id,
                        2,
                        float(index + 1),
                        100 + index,
                        1,
                        "a" * 40,
                        "b" * 64,
                        "toronto-raw-snapshot-v2-archive-2026-11",
                        f"toronto-raw-snapshot-v2-bundle-{snapshot_id}.zip",
                        "succeeded",
                        None,
                        updated,
                    ),
                )

    ledger.start_generation(
        "duckdb",
        generation_id,
        mode="rebuild",
        history_scope=history_scope,
        selected_snapshot_count=2,
        candidate=f"candidate-{generation_id}.duckdb",
    )
    if terminal_status != "RUNNING":
        ledger.finish_generation(
            "duckdb",
            generation_id,
            terminal_status,
            error="synthetic failed generation"
            if terminal_status == "FAILED"
            else None,
        )
    return ledger


def test_mode1_bootstrap_copies_only_a_succeeded_full_mode2_generation(tmp_path):
    source_path = tmp_path / "mode2.sqlite"
    working_path = tmp_path / "mode1-working.sqlite"
    source = _create_accepted_mode2_ledger(source_path)

    result = ReplayLedger.bootstrap_working_ledger(
        source_path,
        working_path,
        "duckdb",
        "mode2-run:1",
    )

    assert result["reused"] is False
    assert result["generation_id"] == "mode2-run:1"
    assert result["candidate"] == "candidate-mode2-run:1.duckdb"
    assert result["selected_snapshot_count"] == 2

    working = ReplayLedger(working_path)
    assert working.bootstrap_record("duckdb")["generation_id"] == "mode2-run:1"
    assert working.completed_snapshot_ids("duckdb") == {
        "snapshot-1",
        "snapshot-2",
    }

    # The source Mode 2 ledger remains unchanged.
    assert source.completed_snapshot_ids("duckdb") == {
        "snapshot-1",
        "snapshot-2",
    }
    assert source.bootstrap_record("duckdb") is None


def test_mode1_bootstrap_reuses_matching_working_ledger_without_overwriting(tmp_path):
    source_path = tmp_path / "mode2.sqlite"
    working_path = tmp_path / "mode1-working.sqlite"
    _create_accepted_mode2_ledger(source_path)

    first = ReplayLedger.bootstrap_working_ledger(
        source_path, working_path, "duckdb", "mode2-run:1"
    )
    second = ReplayLedger.bootstrap_working_ledger(
        source_path, working_path, "duckdb", "mode2-run:1"
    )

    assert first["reused"] is False
    assert second["reused"] is True
    assert second["bootstrapped_at_utc"] == first["bootstrapped_at_utc"]


@pytest.mark.parametrize(
    ("history_scope", "terminal_status", "message"),
    [
        ("partial", "SUCCEEDED", "full-history"),
        ("full", "FAILED", "not SUCCEEDED"),
        ("full", "RUNNING", "not SUCCEEDED"),
    ],
)
def test_mode1_bootstrap_rejects_unaccepted_mode2_generations(
    tmp_path, history_scope, terminal_status, message
):
    source_path = tmp_path / "mode2.sqlite"
    working_path = tmp_path / "mode1-working.sqlite"
    _create_accepted_mode2_ledger(
        source_path,
        history_scope=history_scope,
        terminal_status=terminal_status,
    )

    with pytest.raises(RuntimeError, match=message):
        ReplayLedger.bootstrap_working_ledger(
            source_path, working_path, "duckdb", "mode2-run:1"
        )

    assert not working_path.exists()


def test_mode1_bootstrap_rejects_incomplete_mode2_source_ledger(tmp_path):
    source_path = tmp_path / "mode2.sqlite"
    working_path = tmp_path / "mode1-working.sqlite"
    _create_accepted_mode2_ledger(source_path)

    with sqlite3.connect(source_path) as connection:
        connection.execute(
            """DELETE FROM replay_loads
               WHERE snapshot_id = 'snapshot-2'
                 AND source_name = 'cleared_permits'"""
        )

    with pytest.raises(RuntimeError, match="exactly the Active and Cleared"):
        ReplayLedger.bootstrap_working_ledger(
            source_path, working_path, "duckdb", "mode2-run:1"
        )

    assert not working_path.exists()


def test_mode1_bootstrap_refuses_existing_unidentified_working_ledger(tmp_path):
    source_path = tmp_path / "mode2.sqlite"
    working_path = tmp_path / "mode1-working.sqlite"
    _create_accepted_mode2_ledger(source_path)

    ReplayLedger(working_path)

    with pytest.raises(RuntimeError, match="no bootstrap identity"):
        ReplayLedger.bootstrap_working_ledger(
            source_path, working_path, "duckdb", "mode2-run:1"
        )


def test_mode1_bootstrap_refuses_a_different_generation_for_existing_ledger(
    tmp_path,
):
    source_path = tmp_path / "mode2.sqlite"
    working_path = tmp_path / "mode1-working.sqlite"
    _create_accepted_mode2_ledger(source_path)

    source = ReplayLedger(source_path)
    source.start_generation(
        "duckdb",
        "mode2-run:2",
        mode="rebuild",
        history_scope="full",
        selected_snapshot_count=2,
        candidate="candidate-mode2-run:2.duckdb",
    )
    source.finish_generation("duckdb", "mode2-run:2", "SUCCEEDED")

    ReplayLedger.bootstrap_working_ledger(
        source_path, working_path, "duckdb", "mode2-run:1"
    )

    with pytest.raises(RuntimeError, match="different accepted Mode 2 generation"):
        ReplayLedger.bootstrap_working_ledger(
            source_path, working_path, "duckdb", "mode2-run:2"
        )


def test_full_rebuild_generation_is_successful_only_after_finalization(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    ledger.start_generation(
        "duckdb",
        "run-1:1",
        mode="rebuild",
        history_scope="full",
        selected_snapshot_count=2,
        candidate="candidate.duckdb",
    )

    running = ledger.generation_record("duckdb", "run-1:1")
    assert running["status"] == "RUNNING"
    assert ledger.latest_successful_full_rebuild("duckdb") is None

    ledger.finish_generation("duckdb", "run-1:1", "SUCCEEDED")

    completed = ledger.latest_successful_full_rebuild("duckdb")
    assert completed["generation_id"] == "run-1:1"
    assert completed["candidate"] == "candidate.duckdb"
    assert completed["selected_snapshot_count"] == 2
    assert completed["status"] == "SUCCEEDED"


def test_backend_generation_outcomes_are_independent(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    for backend in ("duckdb", "snowflake"):
        ledger.start_generation(
            backend,
            "run-2:1",
            mode="rebuild",
            history_scope="full",
            selected_snapshot_count=2,
            candidate=f"{backend}-candidate",
        )

    ledger.finish_generation("duckdb", "run-2:1", "SUCCEEDED")
    ledger.finish_generation(
        "snowflake", "run-2:1", "FAILED", error="Snowflake unavailable"
    )

    assert ledger.latest_successful_full_rebuild("duckdb")["status"] == "SUCCEEDED"
    assert ledger.latest_successful_full_rebuild("snowflake") is None
    assert (
        ledger.generation_record("snowflake", "run-2:1")["error"]
        == "Snowflake unavailable"
    )


def test_partial_failed_or_interrupted_generation_is_not_a_full_baseline(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")

    ledger.start_generation(
        "duckdb",
        "partial:1",
        mode="rebuild",
        history_scope="partial",
        selected_snapshot_count=1,
        candidate="partial.duckdb",
    )
    ledger.finish_generation("duckdb", "partial:1", "SUCCEEDED")

    ledger.start_generation(
        "duckdb",
        "failed:1",
        mode="rebuild",
        history_scope="full",
        selected_snapshot_count=2,
        candidate="failed.duckdb",
    )
    ledger.finish_generation(
        "duckdb", "failed:1", "FAILED", error="SCD2 validation failed"
    )

    ledger.start_generation(
        "duckdb",
        "interrupted:1",
        mode="rebuild",
        history_scope="full",
        selected_snapshot_count=2,
        candidate="interrupted.duckdb",
    )

    assert ledger.generation_record("duckdb", "interrupted:1")["status"] == "RUNNING"
    assert ledger.latest_successful_full_rebuild("duckdb") is None


def test_succeeded_generation_cannot_be_restarted_or_downgraded(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    details = {
        "mode": "rebuild",
        "history_scope": "full",
        "selected_snapshot_count": 1,
        "candidate": "candidate.duckdb",
    }
    ledger.start_generation("duckdb", "run-3:1", **details)
    ledger.finish_generation("duckdb", "run-3:1", "SUCCEEDED")

    with pytest.raises(RuntimeError, match="cannot be restarted"):
        ledger.start_generation("duckdb", "run-3:1", **details)

    with pytest.raises(RuntimeError, match="cannot be downgraded"):
        ledger.finish_generation("duckdb", "run-3:1", "FAILED")


def test_identity_change_is_rejected(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    archive = _archive("snapshot-1", 1)
    ledger.register_archives("duckdb", [archive])
    changed = _archive("snapshot-1", 2)

    with pytest.raises(ValueError, match="identity conflicts"):
        ledger.register_archives("duckdb", [changed])


def test_older_snapshot_cannot_be_replayed_after_newer_write_started(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    older, newer = _archive("snapshot-1", 1), _archive("snapshot-2", 2)
    ledger.register_archives("duckdb", [older, newer])
    ledger.mark("duckdb", "active_permits", "snapshot-2", "loading")

    with pytest.raises(RuntimeError, match="Out-of-order"):
        ledger.assert_can_load("duckdb", "active_permits", "snapshot-1")


def test_older_snapshot_is_blocked_when_newer_load_failed_after_start(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    older, newer = _archive("snapshot-1", 1), _archive("snapshot-2", 2)
    ledger.register_archives("duckdb", [older, newer])
    ledger.mark("duckdb", "active_permits", "snapshot-2", "loading")
    ledger.mark(
        "duckdb",
        "active_permits",
        "snapshot-2",
        "failed",
        error="write uncertain",
    )

    with pytest.raises(RuntimeError, match="Out-of-order"):
        ledger.assert_can_load("duckdb", "active_permits", "snapshot-1")


def test_success_cannot_be_downgraded(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    ledger.register_archives("duckdb", [_archive("snapshot-1", 1)])
    _succeed(ledger, "duckdb", "active_permits", "snapshot-1")

    with pytest.raises(RuntimeError, match="cannot be downgraded"):
        ledger.mark("duckdb", "active_permits", "snapshot-1", "failed")


def test_v2_ledger_order_uses_source_extraction_start_not_completion(tmp_path):
    ledger_path = tmp_path / "state.sqlite"
    ledger = ReplayLedger(ledger_path)
    archive = _archive("snapshot-v2", 1)
    ledger.register_archives("duckdb", [archive])

    import sqlite3

    with sqlite3.connect(ledger_path) as connection:
        values = connection.execute(
            "SELECT order_timestamp FROM replay_loads "
            "WHERE backend = 'duckdb' AND snapshot_id = 'snapshot-v2'"
        ).fetchall()

    assert len(values) == 2
    assert all(
        value[0] == pytest.approx(archive.extracted_at.timestamp()) for value in values
    )


def test_latest_successful_snapshot_waits_for_both_source_streams(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    first = _archive("snapshot-1", 1)
    second = _archive("snapshot-2", 2)
    ledger.register_archives("duckdb", [first, second])

    _succeed(ledger, "duckdb", "active_permits", "snapshot-1")
    _succeed(ledger, "duckdb", "cleared_permits", "snapshot-1")
    _succeed(ledger, "duckdb", "active_permits", "snapshot-2")

    baseline = ledger.latest_successful_snapshot("duckdb")
    assert baseline is not None
    assert baseline["snapshot_id"] == "snapshot-1"
    assert set(baseline["source_records"]) == set(SOURCE_NAMES)
    assert {row["status"] for row in baseline["source_records"].values()} == {
        "succeeded"
    }

    _succeed(ledger, "duckdb", "cleared_permits", "snapshot-2")

    baseline = ledger.latest_successful_snapshot("duckdb")
    assert baseline is not None
    assert baseline["snapshot_id"] == "snapshot-2"


def test_latest_successful_snapshot_is_backend_specific(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    archive = _archive("snapshot-1", 1)
    ledger.register_archives("duckdb", [archive])
    ledger.register_archives("snowflake", [archive])

    _succeed(ledger, "duckdb", "active_permits", "snapshot-1")
    _succeed(ledger, "duckdb", "cleared_permits", "snapshot-1")
    _succeed(ledger, "snowflake", "active_permits", "snapshot-1")

    duckdb_baseline = ledger.latest_successful_snapshot("duckdb")
    snowflake_baseline = ledger.latest_successful_snapshot("snowflake")

    assert duckdb_baseline is not None
    assert duckdb_baseline["snapshot_id"] == "snapshot-1"
    assert snowflake_baseline is None


def test_latest_successful_snapshot_rejects_conflicting_source_identity(tmp_path):
    ledger_path = tmp_path / "state.sqlite"
    ledger = ReplayLedger(ledger_path)
    archive = _archive("snapshot-1", 1)
    ledger.register_archives("duckdb", [archive])
    _succeed(ledger, "duckdb", "active_permits", "snapshot-1")
    _succeed(ledger, "duckdb", "cleared_permits", "snapshot-1")

    with sqlite3.connect(ledger_path) as connection:
        connection.execute(
            """UPDATE replay_loads
               SET asset_name = 'different-asset.zip'
               WHERE backend = 'duckdb'
                 AND source_name = 'cleared_permits'
                 AND snapshot_id = 'snapshot-1'"""
        )

    with pytest.raises(RuntimeError, match="disagree on immutable archive identity"):
        ledger.latest_successful_snapshot("duckdb")


def test_snapshot_records_export_persisted_identity_and_source_status(tmp_path):
    ledger = ReplayLedger(tmp_path / "state.sqlite")
    archive = _archive("snapshot-1", 1)
    ledger.register_archives("duckdb", [archive])
    _succeed(ledger, "duckdb", "active_permits", "snapshot-1")
    _succeed(ledger, "duckdb", "cleared_permits", "snapshot-1")

    records = ledger.snapshot_records("duckdb")

    assert len(records) == 2
    assert {row["source_name"] for row in records} == set(SOURCE_NAMES)
    assert {row["snapshot_id"] for row in records} == {"snapshot-1"}
    assert {row["archive_sha256"] for row in records} == {archive.archive_sha256}
    assert {row["status"] for row in records} == {"succeeded"}
