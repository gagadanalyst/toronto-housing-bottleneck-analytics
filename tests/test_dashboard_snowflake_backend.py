from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from dashboard import snowflake_backend as backend

CONFIG = {
    "account": "example-account",
    "user": "dashboard_user",
    "private_key": "fake-pem",
}


class FakePrivateKey:
    def private_bytes(
        self,
        encoding: object,
        format: object,
        encryption_algorithm: object,
    ) -> bytes:
        return b"test-private-key-der"


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.fake_cursor = cursor
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self.fake_cursor

    def close(self) -> None:
        self.closed = True


class FakeCursor:
    def __init__(self, relations: set[str]) -> None:
        self.relations = relations
        self.description: list[tuple[str]] | None = None
        self.rows: list[tuple[Any, ...]] = []
        self.closed = False

    def execute(self, query: str, params: object = None) -> None:
        if "INFORMATION_SCHEMA.TABLES" in query:
            self.rows = [(name,) for name in sorted(self.relations)]
        elif "MART_ACTIVE_PERMIT_AGE" in query:
            self.description = [("PERMIT_NUM",), ("IS_AGE_ELIGIBLE",)]
            self.rows = [("00123", True)]
        elif "MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE" in query:
            self.description = [
                ("APPLICATION_YEAR",),
                ("PERMIT_TYPE",),
                ("MEASURE",),
            ]
            self.rows = [(2025, "Residential", "Application to issue")]
        elif "FCT_CLEARED_PERMIT_REVISION" in query:
            self.description = [("CLEARED_REVISION_COUNT",)] + [
                (field.upper(),) for field in backend.FLAG_FIELDS
            ]
            counts = tuple(
                1 if field == "builder_name_conflict" else 0
                for field in backend.FLAG_FIELDS
            )
            self.rows = [(3, *counts)]
        else:
            raise AssertionError(f"Unexpected SQL query: {query}")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def close(self) -> None:
        self.closed = True


def install_snowflake_stubs(
    monkeypatch: pytest.MonkeyPatch,
    connection: FakeConnection,
) -> dict[str, Any]:
    """Provide fake optional dependencies and a fake Snowflake connection."""
    serialization = ModuleType("cryptography.hazmat.primitives.serialization")
    serialization.Encoding = SimpleNamespace(DER="DER")
    serialization.PrivateFormat = SimpleNamespace(PKCS8="PKCS8")
    serialization.NoEncryption = lambda: "no-encryption"
    serialization.load_pem_private_key = lambda key, password: FakePrivateKey()

    cryptography = ModuleType("cryptography")
    cryptography.__path__ = []
    hazmat = ModuleType("cryptography.hazmat")
    hazmat.__path__ = []
    primitives = ModuleType("cryptography.hazmat.primitives")
    primitives.__path__ = []
    cryptography.hazmat = hazmat
    hazmat.primitives = primitives
    primitives.serialization = serialization

    connector = ModuleType("snowflake.connector")
    connection_args: dict[str, Any] = {}

    def connect(**kwargs: Any) -> FakeConnection:
        connection_args.update(kwargs)
        return connection

    connector.connect = connect
    snowflake = ModuleType("snowflake")
    snowflake.__path__ = []
    snowflake.connector = connector

    modules = {
        "cryptography": cryptography,
        "cryptography.hazmat": hazmat,
        "cryptography.hazmat.primitives": primitives,
        "cryptography.hazmat.primitives.serialization": serialization,
        "snowflake": snowflake,
        "snowflake.connector": connector,
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    return connection_args


def test_missing_configuration_fails_before_connection() -> None:
    with pytest.raises(ValueError, match="missing: account, user, private_key"):
        backend.load_snowflake_dashboard_data({})


def test_invalid_database_identifier_is_rejected() -> None:
    config = {**CONFIG, "database": 'TORONTO"; DROP DATABASE TORONTO; --'}

    with pytest.raises(ValueError, match="Invalid Snowflake database identifier"):
        backend.load_snowflake_dashboard_data(config)


def test_missing_relations_fail_and_close_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = FakeCursor({"MART_ACTIVE_PERMIT_AGE"})
    connection = FakeConnection(cursor)
    install_snowflake_stubs(monkeypatch, connection)

    with pytest.raises(RuntimeError, match="missing required dashboard relations"):
        backend.load_snowflake_dashboard_data(CONFIG)

    assert cursor.closed
    assert connection.closed


def test_successful_read_returns_dashboard_contract_and_closes_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = FakeCursor(set(backend.REQUIRED_RELATIONS))
    connection = FakeConnection(cursor)
    connection_args = install_snowflake_stubs(monkeypatch, connection)

    active, summary, cleared_count, flags = backend.load_snowflake_dashboard_data(
        CONFIG
    )

    assert active.loc[0, "permit_num"] == "00123"
    assert bool(active.loc[0, "is_age_eligible"])
    assert summary.loc[0, "permit_type"] == "Residential"
    assert cleared_count == 3
    assert flags["builder_name_conflict"] == 1
    assert all(
        flags[field] == 0
        for field in backend.FLAG_FIELDS
        if field != "builder_name_conflict"
    )
    assert connection_args["role"] == "TORONTO_DASHBOARD_ROLE"
    assert connection_args["warehouse"] == "TORONTO_XS_WH"
    assert connection_args["database"] == "TORONTO"
    assert cursor.closed
    assert connection.closed
