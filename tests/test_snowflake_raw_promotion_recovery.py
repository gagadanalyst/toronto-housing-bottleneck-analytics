"""Regression tests for interrupted Snowflake RAW schema-swap recovery."""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

# The promotion module imports its DLT state helpers at module import time. Keep
# this unit test independent of the DLT runtime; the helpers are patched below.
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
