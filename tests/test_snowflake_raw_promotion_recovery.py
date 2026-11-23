"""Regression tests for interrupted Snowflake RAW schema-swap recovery."""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

# The promotion module imports its DLT state helpers at module import time. Keep
# this unit test independent of the DLT runtime; the helpers are patched below.
# Keep these recovery tests independent of the optional Snowflake connector.
if "snowflake.connector" not in sys.modules:
    try:
        import snowflake.connector  # noqa: F401
    except ModuleNotFoundError:
        snowflake_package = ModuleType("snowflake")
        snowflake_package.__path__ = []
        connector_module = ModuleType("snowflake.connector")

        def _no_live_connection(**kwargs):
            raise AssertionError("Offline tests must not open Snowflake connections")

        connector_module.connect = _no_live_connection
        snowflake_package.connector = connector_module
        sys.modules["snowflake"] = snowflake_package
        sys.modules["snowflake.connector"] = connector_module

_state_helpers = ModuleType("snowflake_candidate_utils")
_state_helpers.decode_state = lambda value, pipeline: {  # type: ignore[attr-defined]
    "dataset_name": "RAW_STAGE_123_2"
}
_state_helpers.rebase_pipeline_state = lambda *args: 2  # type: ignore[attr-defined]
_existing_helpers = sys.modules.get("snowflake_candidate_utils")
sys.modules["snowflake_candidate_utils"] = _state_helpers
try:
    from scripts import promote_snowflake_raw_candidate as promotion
finally:
    if _existing_helpers is None:
        sys.modules.pop("snowflake_candidate_utils", None)
    else:
        sys.modules["snowflake_candidate_utils"] = _existing_helpers

# Import after installing the test stub in sys.modules.
from scripts import prepare_snowflake_raw_candidate as preparation  # noqa: E402


class _FakeCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, statement: str, params=None) -> None:
        self.executed.append(statement)

    def fetchall(self):
        statement = self.executed[-1]
        if "SCHEMA_OWNER" in statement:
            return [
                ("RAW", "TORONTO_INGEST_ROLE"),
                ("RAW_STAGE_123_2", "TORONTO_INGEST_ROLE"),
            ]
        if "GENERATION_ID" in statement:
            return [("123:2", "PROMOTED")]
        if "SELECT STATE FROM TORONTO.RAW._DLT_PIPELINE_STATE" in statement:
            return [("encoded-state",)]
        raise AssertionError(f"Unexpected fetchall query: {statement}")

    def fetchone(self):
        statement = self.executed[-1]
        if "COUNT(*) FROM TORONTO.INFORMATION_SCHEMA.TABLES" in statement:
            return (1,)
        raise AssertionError(f"Unexpected fetchone query: {statement}")

    def close(self) -> None:
        pass


class _FakeConnection:
    def __init__(self) -> None:
        self.fake_cursor = _FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.fake_cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        pass


def test_failed_validation_of_already_swapped_raw_restores_previous_schema(
    monkeypatch,
) -> None:
    connection = _FakeConnection()
    rebase_calls: list[tuple[str, str, str]] = []

    monkeypatch.setattr(promotion, "_options", lambda uri, key_path: {})
    monkeypatch.setattr(
        promotion.snowflake.connector,
        "connect",
        lambda **kwargs: connection,
    )
    monkeypatch.setattr(
        promotion,
        "_control_generation",
        lambda cursor, schema: ("123:2", "PROMOTED"),
    )
    monkeypatch.setattr(
        promotion,
        "decode_state",
        lambda value, pipeline: {"dataset_name": "RAW_STAGE_123_2"},
    )

    def rebase(cursor, schema, source_dataset, target_dataset):
        rebase_calls.append((schema, source_dataset, target_dataset))
        return 2

    monkeypatch.setattr(promotion, "rebase_pipeline_state", rebase)
    monkeypatch.setattr(
        promotion,
        "_assert_scd2",
        lambda *args: (_ for _ in ()).throw(
            RuntimeError("injected post-swap validation failure")
        ),
    )

    with pytest.raises(RuntimeError, match="injected post-swap validation failure"):
        promotion.promote(
            "not-used-by-mocked-connection",
            "RAW_STAGE_123_2",
            "123:2",
            {"active_permits": 100, "cleared_permits": 100},
            set(),
        )

    swaps = [
        statement
        for statement in connection.fake_cursor.executed
        if "ALTER SCHEMA TORONTO.RAW SWAP" in statement
    ]
    assert len(swaps) == 1  # rollback to the previous production RAW
    assert rebase_calls == [
        ("RAW", "RAW_STAGE_123_2", "RAW"),
        ("RAW_STAGE_123_2", "RAW", "RAW_STAGE_123_2"),
    ]
    assert connection.commits == 3
    assert connection.rollbacks == 0


def test_dry_run_refuses_interrupted_raw_swap_without_recovery(monkeypatch):
    class Cursor:
        def __init__(self):
            self.statements = []

        def execute(self, statement, params=None):
            self.statements.append(statement)

        def fetchall(self):
            return [("toronto_active_permits_scd2", "encoded-state")]

        def fetchone(self):
            return (1,)

    cursor = Cursor()
    monkeypatch.setattr(
        preparation,
        "decode_state",
        lambda value, pipeline: {"dataset_name": "RAW_STAGE_123_2"},
    )
    monkeypatch.setattr(
        preparation,
        "rebase_pipeline_state",
        lambda *args: pytest.fail("Dry run must not rebase canonical DLT state."),
    )

    with pytest.raises(RuntimeError, match="refusing to modify canonical TORONTO.RAW"):
        preparation._recover_interrupted_raw_swap(cursor, dry_run=True)

    assert len(cursor.statements) == 2
    assert all(
        statement.lstrip().upper().startswith("SELECT")
        for statement in cursor.statements
    )


def test_snowflake_validate_only_reads_candidate_without_swapping(monkeypatch):
    class Cursor:
        def __init__(self):
            self.statements = []

        def execute(self, statement, params=None):
            self.statements.append(statement)

        def fetchone(self):
            return ("TORONTO_INGEST_ROLE", "TORONTO", "RAW")

        def fetchall(self):
            statement = self.statements[-1]
            if "SCHEMA_NAME, SCHEMA_OWNER" in statement:
                return [("RAW_STAGE_123_2", "TORONTO_INGEST_ROLE")]
            if "GENERATION_ID" in statement:
                return [("123:2", "READY")]
            raise AssertionError(f"Unexpected query: {statement}")

        def close(self):
            pass

    class Connection:
        def __init__(self):
            self.value = Cursor()
            self.closed = False

        def cursor(self):
            return self.value

        def close(self):
            self.closed = True

    connection = Connection()
    validated = []
    monkeypatch.setattr(promotion, "_options", lambda uri, key_path: {})
    monkeypatch.setattr(
        promotion.snowflake.connector,
        "connect",
        lambda **kwargs: connection,
    )
    monkeypatch.setattr(
        promotion,
        "_validate_candidate",
        lambda cursor, stage, counts, snapshots: validated.append(
            (stage, counts, snapshots)
        ),
    )

    promotion.validate_candidate_only(
        "unused",
        "RAW_STAGE_123_2",
        "123:2",
        {"active_permits": 100, "cleared_permits": 200},
        {"123:1"},
    )

    assert validated == [
        ("RAW_STAGE_123_2", {"active_permits": 100, "cleared_permits": 200}, {"123:1"})
    ]
    assert connection.closed
    assert all(
        statement.lstrip().upper().startswith("SELECT")
        for statement in connection.value.statements
    )
    assert not any(
        token in statement.upper()
        for statement in connection.value.statements
        for token in ("ALTER ", "UPDATE ", "INSERT ", "DELETE ", "DROP ", "GRANT ")
    )


def test_failed_candidate_preparation_drops_only_its_new_owned_clone(monkeypatch):
    class Cursor:
        def __init__(self):
            self.statements = []
            self.one = None
            self.all = []

        def execute(self, statement, params=None):
            self.statements.append(statement)
            upper = statement.upper()

            if "SELECT CURRENT_ROLE()" in upper:
                self.one = ("TORONTO_INGEST_ROLE", "TORONTO", "RAW")
            elif "SELECT SCHEMA_NAME FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA" in upper:
                self.one = None
            elif "COUNT_IF(_DLT_VALID_TO IS NULL)" in upper:
                self.one = (100,)
            elif "SELECT SNAPSHOT_ID, SOURCE_NAME, STATUS" in upper:
                self.all = []
            elif (
                upper.startswith("CREATE TABLE") and "TORONTO_SCD2_QUARANTINE" in upper
            ):
                raise RuntimeError("simulated candidate preparation failure")
            elif (
                "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA" in upper
            ):
                self.one = ("TORONTO_INGEST_ROLE",)

        def fetchone(self):
            return self.one

        def fetchall(self):
            return self.all

        def close(self):
            pass

    class Connection:
        def __init__(self):
            self.value = Cursor()
            self.commits = 0
            self.rollbacks = 0
            self.closed = False

        def cursor(self):
            return self.value

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

        def close(self):
            self.closed = True

    connection = Connection()

    monkeypatch.setattr(preparation, "_connection_options", lambda uri, key: {})
    monkeypatch.setattr(
        preparation,
        "_recover_interrupted_raw_swap",
        lambda cursor, dry_run=False: 0,
    )
    monkeypatch.setattr(
        preparation.snowflake.connector,
        "connect",
        lambda **kwargs: connection,
    )

    with pytest.raises(
        RuntimeError,
        match="simulated candidate preparation failure",
    ):
        preparation.prepare("unused", "123", "2", None, dry_run=True)

    statements = connection.value.statements

    assert any(
        statement.startswith('CREATE SCHEMA TORONTO."RAW_STAGE_123_2" CLONE')
        for statement in statements
    )
    assert any(
        statement.startswith('DROP SCHEMA TORONTO."RAW_STAGE_123_2" CASCADE')
        for statement in statements
    )
    assert not any(
        statement.startswith('DROP SCHEMA TORONTO."RAW"') for statement in statements
    )
    assert connection.commits == 1
    assert connection.rollbacks == 1
    assert connection.closed


@pytest.mark.parametrize("status", ["succeeded", "excluded"])
def test_quarantine_ledger_accepts_excluded_status(status) -> None:
    class Cursor:
        def __init__(self):
            self.statement = ""
            self.ledger_rows = [
                ("snapshot-1", "active_permits", 0, 0, 0, None, status),
                ("snapshot-1", "cleared_permits", 0, 0, 0, None, "succeeded"),
            ]

        def execute(self, statement, params=None):
            self.statement = statement

        def fetchall(self):
            if "TORONTO_SCD2_QUARANTINE" in self.statement:
                return []
            if "TORONTO_SNAPSHOT_LEDGER" in self.statement:
                return self.ledger_rows
            raise AssertionError(f"Unexpected query: {self.statement}")

    promotion._assert_quarantine_ledger(
        Cursor(),
        "RAW_STAGE_123_2",
        {"active_permits": 100, "cleared_permits": 100},
        {"snapshot-1"},
    )


@pytest.mark.parametrize(
    "status",
    ["pending", "loading", "failed", None, "unknown"],
)
def test_quarantine_ledger_rejects_nonterminal_status(status) -> None:
    class Cursor:
        def __init__(self):
            self.statement = ""
            self.ledger_rows = [
                ("snapshot-1", "active_permits", 0, 0, 0, None, status),
                ("snapshot-1", "cleared_permits", 0, 0, 0, None, "succeeded"),
            ]

        def execute(self, statement, params=None):
            self.statement = statement

        def fetchall(self):
            if "TORONTO_SCD2_QUARANTINE" in self.statement:
                return []
            if "TORONTO_SNAPSHOT_LEDGER" in self.statement:
                return self.ledger_rows
            raise AssertionError(f"Unexpected query: {self.statement}")

    with pytest.raises(RuntimeError, match="unfinished or unsupported source"):
        promotion._assert_quarantine_ledger(
            Cursor(),
            "RAW_STAGE_123_2",
            {"active_permits": 100, "cleared_permits": 100},
            {"snapshot-1"},
        )
