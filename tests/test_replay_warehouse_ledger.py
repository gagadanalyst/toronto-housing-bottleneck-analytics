from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import replay_warehouse_ledger


def _archive(snapshot_id: str, minute: int, *, run_id: int) -> SimpleNamespace:
    started = datetime(2026, 11, 1, 12, minute, tzinfo=UTC)
    source = SimpleNamespace(
        extracted_at=started + timedelta(seconds=2),
        manifest={},
        parquet_sha256=f"{minute:064x}",
        row_count=3000,
        columns=("permit_num", "revision_num", "permit_type"),
        error=None,
    )
    return SimpleNamespace(
        path=Path(f"{snapshot_id}.zip"),
        format_version=2,
        snapshot_id=snapshot_id,
        extracted_at=started,
        source_commit="a" * 40,
        run_id=str(run_id),
        run_attempt=1,
        release_tag="toronto-raw-snapshot-v2-archive-2026-11",
        asset_name=f"toronto-raw-snapshot-v2-bundle-{snapshot_id}.zip",
        archive_sha256="b" * 64,
        sources={"active_permits": source, "cleared_permits": source},
        order=(started, run_id, 1, snapshot_id),
    )


class _DuckDBLedgerTarget:
    backend = "duckdb"

    def __init__(self, database: Path):
        self.database = database
        self.dataset = "raw"


def _target(path: Path) -> _DuckDBLedgerTarget:
    target = _DuckDBLedgerTarget(path)
    replay_warehouse_ledger.ensure_ledger(target)
    return target


def test_native_ledger_registers_both_sources_and_tracks_completion(tmp_path):
    target = _target(tmp_path / "candidate.duckdb")
    archive = _archive("snapshot-a", 1, run_id=1001)

    target_archive = replay_warehouse_ledger.register_archive
    target_archive(target, archive)

    assert (
        replay_warehouse_ledger.snapshot_status(
            target, archive.snapshot_id, "active_permits"
        )
        == "pending"
    )
    assert (
        replay_warehouse_ledger.snapshot_status(
            target, archive.snapshot_id, "cleared_permits"
        )
        == "pending"
    )
    assert replay_warehouse_ledger.completed_snapshot_ids(target) == set()

    for source_name in ("active_permits", "cleared_permits"):
        replay_warehouse_ledger.set_snapshot_status(
            target, archive, source_name, "loading"
        )
        replay_warehouse_ledger.set_snapshot_status(
            target, archive, source_name, "succeeded"
        )

    assert replay_warehouse_ledger.completed_snapshot_ids(target) == {
        archive.snapshot_id
    }


def test_native_ledger_retry_preserves_success_and_resets_only_failed_source(tmp_path):
    target = _target(tmp_path / "candidate.duckdb")
    archive = _archive("snapshot-a", 1, run_id=1001)
    replay_warehouse_ledger.register_archive(target, archive)

    replay_warehouse_ledger.set_snapshot_status(
        target, archive, "active_permits", "loading"
    )
    replay_warehouse_ledger.set_snapshot_status(
        target, archive, "active_permits", "succeeded"
    )
    replay_warehouse_ledger.set_snapshot_status(
        target, archive, "cleared_permits", "loading"
    )
    replay_warehouse_ledger.set_snapshot_status(
        target, archive, "cleared_permits", "failed", error="injected failure"
    )

    replay_warehouse_ledger.register_archive(target, archive)

    assert (
        replay_warehouse_ledger.snapshot_status(
            target, archive.snapshot_id, "active_permits"
        )
        == "succeeded"
    )
    assert (
        replay_warehouse_ledger.snapshot_status(
            target, archive.snapshot_id, "cleared_permits"
        )
        == "pending"
    )


def test_native_ledger_enforces_chronology_independently_per_source(tmp_path):
    target = _target(tmp_path / "candidate.duckdb")
    older = _archive("snapshot-older", 1, run_id=1001)
    newer = _archive("snapshot-newer", 2, run_id=1002)
    replay_warehouse_ledger.register_archive(target, older)
    replay_warehouse_ledger.register_archive(target, newer)

    replay_warehouse_ledger.set_snapshot_status(
        target, older, "active_permits", "loading"
    )
    replay_warehouse_ledger.set_snapshot_status(
        target, older, "active_permits", "succeeded"
    )
    replay_warehouse_ledger.set_snapshot_status(
        target, older, "cleared_permits", "loading"
    )
    replay_warehouse_ledger.set_snapshot_status(
        target, older, "cleared_permits", "failed", error="injected"
    )

    # Active may advance because its prior snapshot succeeded.
    replay_warehouse_ledger.assert_replay_order(target, newer, "active_permits")
    # Cleared remains blocked by its own failed earlier snapshot.
    with pytest.raises(RuntimeError, match="earlier incomplete"):
        replay_warehouse_ledger.assert_replay_order(target, newer, "cleared_permits")


def test_native_ledger_blocks_late_older_snapshot_after_newer_write(tmp_path):
    target = _target(tmp_path / "candidate.duckdb")
    older = _archive("snapshot-older", 1, run_id=1001)
    newer = _archive("snapshot-newer", 2, run_id=1002)
    replay_warehouse_ledger.register_archive(target, older)
    replay_warehouse_ledger.register_archive(target, newer)

    replay_warehouse_ledger.set_snapshot_status(
        target, newer, "active_permits", "loading"
    )
    replay_warehouse_ledger.set_snapshot_status(
        target, newer, "active_permits", "failed", error="write may have committed"
    )

    with pytest.raises(RuntimeError, match="Out-of-order"):
        replay_warehouse_ledger.assert_replay_order(target, older, "active_permits")


def test_native_ledger_rejects_changed_immutable_archive_identity(tmp_path):
    target = _target(tmp_path / "candidate.duckdb")
    archive = _archive("snapshot-a", 1, run_id=1001)
    replay_warehouse_ledger.register_archive(target, archive)
    changed = _archive("snapshot-a", 1, run_id=1001)
    changed.archive_sha256 = "c" * 64

    with pytest.raises(ValueError, match="identity conflicts"):
        replay_warehouse_ledger.register_archive(target, changed)


def test_legacy_v1_ledger_identity_allows_missing_release_fields():
    archive = _archive("123456:1", 1, run_id=123456)
    values = replay_warehouse_ledger._values(archive, "active_permits")
    old = (
        archive.run_id,
        archive.run_attempt,
        archive.extracted_at,
        archive.source_commit,
        None,
        None,
        "succeeded",
        None,
        None,
    )

    replay_warehouse_ledger._assert_identity(old, values, "DuckDB")


def test_legacy_v1_ledger_identity_still_rejects_changed_release_fields():
    archive = _archive("123456:1", 1, run_id=123456)
    values = replay_warehouse_ledger._values(archive, "active_permits")
    old = (
        archive.run_id,
        archive.run_attempt,
        archive.extracted_at,
        archive.source_commit,
        "toronto-raw-snapshot-archive-2026-10",
        archive.asset_name,
        "succeeded",
        1,
        None,
    )

    with pytest.raises(ValueError, match="identity conflicts"):
        replay_warehouse_ledger._assert_identity(old, values, "DuckDB")
