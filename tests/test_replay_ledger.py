from __future__ import annotations

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
