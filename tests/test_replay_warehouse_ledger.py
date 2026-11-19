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
        target,
        archive,
        "active_permits",
        "succeeded",
        result={
            "quarantined_rows": 2,
            "quarantined_bad_rows": 1,
            "quarantined_business_keys": 1,
            "quarantine_sha256": "c" * 64,
        },
    )
    replay_warehouse_ledger.set_snapshot_status(
        target, archive, "cleared_permits", "loading"
    )
    replay_warehouse_ledger.set_snapshot_status(
        target, archive, "cleared_permits", "failed", error="injected failure"
    )

    replay_warehouse_ledger.register_archive(target, archive)

    records = replay_warehouse_ledger.snapshot_records(target)
    active = next(
        record
        for record in records
        if record["snapshot_id"] == archive.snapshot_id
        and record["source_name"] == "active_permits"
    )
    assert active["status"] == "succeeded"
    assert active["quarantined_row_count"] == 2
    assert active["quarantined_bad_row_count"] == 1
    assert active["quarantined_business_key_count"] == 1
    assert active["quarantine_sha256"] == "c" * 64

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


def _baseline_target(tmp_path, *, missing_anchor=False):
    target = _target(tmp_path / "candidate.duckdb")
    archives = []

    for index, run_id in enumerate(range(42022028598, 42022028626), start=1):
        if missing_anchor and run_id == 42022028625:
            continue

        archive = _archive(f"{run_id}:1", index, run_id=run_id)
        archives.append(archive)
        replay_warehouse_ledger.register_archive(target, archive)

        for source in ("active_permits", "cleared_permits"):
            replay_warehouse_ledger.set_snapshot_status(
                target, archive, source, "succeeded"
            )

    return target, archives


def test_baseline_preflight_accepts_28_snapshots_and_later_pending_snapshot(tmp_path):
    target, _ = _baseline_target(tmp_path)
    later = _archive("after-baseline", 29, run_id=42022028626)
    replay_warehouse_ledger.register_archive(target, later)

    result = replay_warehouse_ledger.validate_canonical_baseline(
        target, "42022028625:1", 28
    )

    assert result["status"] == "passed"
    assert result["snapshot_count"] == 28


@pytest.mark.parametrize(
    ("case", "expected_count", "error"),
    [
        ("missing_anchor", 28, "anchor"),
        ("wrong_count", 27, "expected 27"),
        ("failed_source", 28, "Both sources must have succeeded"),
    ],
)
def test_baseline_preflight_rejects_invalid_baseline(
    tmp_path, case, expected_count, error
):
    target, archives = _baseline_target(
        tmp_path, missing_anchor=case == "missing_anchor"
    )

    if case == "failed_source":
        replay_warehouse_ledger.set_snapshot_status(
            target,
            archives[10],
            "cleared_permits",
            "failed",
            error="test failure",
        )

    with pytest.raises(RuntimeError, match=error):
        replay_warehouse_ledger.validate_canonical_baseline(
            target, "42022028625:1", expected_count
        )


def test_baseline_preflight_rejects_duplicate_snowflake_source_entry():
    from contextlib import nullcontext

    rows = [
        (
            archive.snapshot_id,
            archive.run_id,
            archive.run_attempt,
            archive.extracted_at,
            source,
            "succeeded",
        )
        for archive in [
            _archive(f"{run_id}:1", index, run_id=run_id)
            for index, run_id in enumerate(range(42022028598, 42022028626), start=1)
        ]
        for source in ("active_permits", "cleared_permits")
    ]
    rows.append(rows[-2])

    class Cursor:
        def execute(self, query):
            pass

        def fetchall(self):
            return rows

        def close(self):
            pass

    cursor = Cursor()

    class Target:
        backend = "snowflake"
        schema = "RAW_STAGE_1_1"

        def _session(self):
            return nullcontext(
                type("Connection", (), {"cursor": lambda self: cursor})()
            )

    with pytest.raises(RuntimeError, match="exactly one entry"):
        replay_warehouse_ledger.validate_canonical_baseline(
            Target(), "42022028625:1", 28
        )
