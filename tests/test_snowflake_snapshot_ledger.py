import sys
from datetime import UTC, datetime
from types import ModuleType

import pytest

# Keep this module independently runnable without the optional Snowflake extra.
if "snowflake.connector" not in sys.modules:
    try:
        import snowflake.connector  # noqa: F401
    except ModuleNotFoundError:
        snowflake_package = ModuleType("snowflake")
        snowflake_package.__path__ = []
        connector_module = ModuleType("snowflake.connector")

        def _no_live_connection(**kwargs):
            raise AssertionError("Ledger unit tests must never connect to Snowflake")

        connector_module.connect = _no_live_connection
        snowflake_package.connector = connector_module
        sys.modules["snowflake"] = snowflake_package
        sys.modules["snowflake.connector"] = connector_module

from scripts import snowflake_refresh_raw as refresh


class FakeCursor:
    def __init__(self, *, existing=(), statuses=None):
        self.existing = list(existing)
        self.statuses = statuses or {}
        self.queries = []
        self.last_status = None
        self.closed = False

    def execute(self, query, params=None):
        self.queries.append((query, params))
        if query.lstrip().upper().startswith("UPDATE"):
            self.last_status = params[0]

    def fetchall(self):
        query = self.queries[-1][0]
        if "SELECT SNAPSHOT_ID" in query:
            return self.existing
        if "SELECT SOURCE_NAME, STATUS" in query:
            return list(self.statuses.items())
        return []

    def fetchone(self):
        return (self.last_status,) if self.last_status else None

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self.fake_cursor = cursor
        self.closed = False

    def cursor(self):
        return self.fake_cursor

    def close(self):
        self.closed = True


def _bundle():
    return {"extraction_started_at_utc": "2026-11-07T15:00:00+00:00"}


def test_prepare_snapshot_ledger_registers_both_sources(monkeypatch):
    cursor = FakeCursor(
        statuses={"active_permits": "pending", "cleared_permits": "pending"}
    )
    connection = FakeConnection(cursor)
    monkeypatch.setattr(
        refresh.snowflake.connector, "connect", lambda **kwargs: connection
    )

    statuses = refresh._prepare_snapshot_ledger(
        {"role": "TORONTO_INGEST_ROLE"},
        bundle=_bundle(),
        run_id="100",
        attempt="1",
        commit="a" * 40,
        release_tag="toronto-raw-snapshot-archive-2026-11",
        asset_name="toronto-raw-snapshot-bundle-100-1.zip",
    )

    assert statuses == {"active_permits": "pending", "cleared_permits": "pending"}
    assert any("CREATE TABLE IF NOT EXISTS" in query for query, _ in cursor.queries)
    assert sum("MERGE INTO" in query for query, _ in cursor.queries) == 2
    assert cursor.closed and connection.closed


def test_prepare_snapshot_ledger_blocks_earlier_incomplete_snapshot(monkeypatch):
    older = (
        "99:1",
        "99",
        1,
        datetime(2026, 11, 7, 14, 59, tzinfo=UTC),
        "active_permits",
        "failed",
    )
    cursor = FakeCursor(existing=[older])
    connection = FakeConnection(cursor)
    monkeypatch.setattr(
        refresh.snowflake.connector, "connect", lambda **kwargs: connection
    )

    with pytest.raises(RuntimeError, match="earlier archived snapshots"):
        refresh._prepare_snapshot_ledger(
            {},
            bundle=_bundle(),
            run_id="100",
            attempt="1",
            commit="a" * 40,
            release_tag="toronto-raw-snapshot-archive-2026-11",
            asset_name="toronto-raw-snapshot-bundle-100-1.zip",
        )
    assert not any("MERGE INTO" in query for query, _ in cursor.queries)
    assert cursor.closed and connection.closed


def test_set_snapshot_source_status_updates_and_verifies(monkeypatch):
    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    monkeypatch.setattr(
        refresh.snowflake.connector, "connect", lambda **kwargs: connection
    )

    refresh._set_snapshot_source_status(
        {},
        snapshot_id="100:1",
        source_name="active_permits",
        status="succeeded",
    )

    assert any(
        "UPDATE TORONTO.RAW.TORONTO_SNAPSHOT_LEDGER" in query
        for query, _ in cursor.queries
    )
    assert cursor.closed and connection.closed


def test_prepare_snapshot_ledger_allows_repeat_of_completed_old_archive(monkeypatch):
    existing = [
        ("100:1", "100", 1, datetime(2026, 11, 7, 15, 0, tzinfo=UTC), name, "succeeded")
        for name in ("active_permits", "cleared_permits")
    ]
    # A later completed row must not make an already-completed retry unsafe.
    existing.append(
        (
            "101:1",
            "101",
            1,
            datetime(2026, 11, 7, 15, 1, tzinfo=UTC),
            "active_permits",
            "succeeded",
        )
    )
    cursor = FakeCursor(existing=existing)
    connection = FakeConnection(cursor)
    monkeypatch.setattr(
        refresh.snowflake.connector, "connect", lambda **kwargs: connection
    )

    statuses = refresh._prepare_snapshot_ledger(
        {},
        bundle=_bundle(),
        run_id="100",
        attempt="1",
        commit="a" * 40,
        release_tag="toronto-raw-snapshot-archive-2026-11",
        asset_name="toronto-raw-snapshot-bundle-100-1.zip",
    )

    assert statuses == {"active_permits": "succeeded", "cleared_permits": "succeeded"}
    assert not any("MERGE INTO" in query for query, _ in cursor.queries)


@pytest.mark.parametrize("later_status", ["loading", "failed"])
def test_prepare_snapshot_ledger_blocks_older_replay_after_newer_write_may_have_committed(
    monkeypatch, later_status
):
    newer = (
        "101:1",
        "101",
        1,
        datetime(2026, 11, 7, 15, 1, tzinfo=UTC),
        "active_permits",
        later_status,
    )
    cursor = FakeCursor(existing=[newer])
    connection = FakeConnection(cursor)
    monkeypatch.setattr(
        refresh.snowflake.connector, "connect", lambda **kwargs: connection
    )

    with pytest.raises(
        RuntimeError, match="newer snapshot has a non-pending source status"
    ):
        refresh._prepare_snapshot_ledger(
            {},
            bundle=_bundle(),
            run_id="100",
            attempt="1",
            commit="a" * 40,
            release_tag="toronto-raw-snapshot-archive-2026-11",
            asset_name="toronto-raw-snapshot-bundle-100-1.zip",
        )
    assert not any("MERGE INTO" in query for query, _ in cursor.queries)
    assert cursor.closed and connection.closed


def test_prepare_snapshot_ledger_retries_only_the_incomplete_source(monkeypatch):
    existing = [
        (
            "100:1",
            "100",
            1,
            datetime(2026, 11, 7, 15, 0, tzinfo=UTC),
            "active_permits",
            "succeeded",
        ),
        (
            "100:1",
            "100",
            1,
            datetime(2026, 11, 7, 15, 0, tzinfo=UTC),
            "cleared_permits",
            "failed",
        ),
    ]
    cursor = FakeCursor(
        existing=existing,
        statuses={"active_permits": "succeeded", "cleared_permits": "pending"},
    )
    connection = FakeConnection(cursor)
    monkeypatch.setattr(
        refresh.snowflake.connector, "connect", lambda **kwargs: connection
    )

    statuses = refresh._prepare_snapshot_ledger(
        {},
        bundle=_bundle(),
        run_id="100",
        attempt="1",
        commit="a" * 40,
        release_tag="toronto-raw-snapshot-archive-2026-11",
        asset_name="toronto-raw-snapshot-bundle-100-1.zip",
    )

    assert statuses == {"active_permits": "succeeded", "cleared_permits": "pending"}
    assert sum("MERGE INTO" in query for query, _ in cursor.queries) == 2


def test_prepare_snapshot_ledger_blocks_when_older_source_row_is_missing(monkeypatch):
    older = (
        "99:1",
        "99",
        1,
        datetime(2026, 11, 7, 14, 59, tzinfo=UTC),
        "active_permits",
        "succeeded",
    )
    cursor = FakeCursor(existing=[older])
    connection = FakeConnection(cursor)
    monkeypatch.setattr(
        refresh.snowflake.connector, "connect", lambda **kwargs: connection
    )

    with pytest.raises(RuntimeError, match="both source loads recorded as succeeded"):
        refresh._prepare_snapshot_ledger(
            {},
            bundle=_bundle(),
            run_id="100",
            attempt="1",
            commit="a" * 40,
            release_tag="toronto-raw-snapshot-archive-2026-11",
            asset_name="toronto-raw-snapshot-bundle-100-1.zip",
        )
    assert not any("MERGE INTO" in query for query, _ in cursor.queries)


def test_completed_snapshot_ids_requires_both_sources_succeeded(monkeypatch):
    rows = [
        ("100:1", "active_permits", "succeeded"),
        ("100:1", "cleared_permits", "succeeded"),
        ("101:1", "active_permits", "succeeded"),
        ("101:1", "cleared_permits", "failed"),
        ("102:1", "active_permits", "succeeded"),
    ]
    cursor = FakeCursor(existing=rows)
    connection = FakeConnection(cursor)
    monkeypatch.setattr(
        refresh.snowflake.connector, "connect", lambda **kwargs: connection
    )

    completed = refresh._completed_snapshot_ids({})

    assert completed == {"100:1"}
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("later_status", ["loading", "failed"])
def test_prepare_snapshot_ledger_blocks_older_replay_when_newer_write_may_have_committed(
    monkeypatch, later_status
):
    later = (
        "101:1",
        "101",
        1,
        datetime(2026, 11, 7, 15, 1, tzinfo=UTC),
        "active_permits",
        later_status,
    )
    cursor = FakeCursor(existing=[later])
    connection = FakeConnection(cursor)
    monkeypatch.setattr(
        refresh.snowflake.connector, "connect", lambda **kwargs: connection
    )

    with pytest.raises(
        RuntimeError, match="newer snapshot has a non-pending source status"
    ):
        refresh._prepare_snapshot_ledger(
            {},
            bundle=_bundle(),
            run_id="100",
            attempt="1",
            commit="a" * 40,
            release_tag="toronto-raw-snapshot-archive-2026-11",
            asset_name="toronto-raw-snapshot-bundle-100-1.zip",
        )
