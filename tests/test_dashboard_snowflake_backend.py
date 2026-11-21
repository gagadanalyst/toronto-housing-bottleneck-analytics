from __future__ import annotations

import sys
from datetime import UTC, datetime
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from dashboard import snowflake_backend as backend

CONFIG = {
    "account": "example-account",
    "user": "dashboard_user",
    "private_key": "fake-pem",
}

QUALITY_METRIC_VALUES = {
    "active_revisions_analysed": 100,
    "active_revisions_age_eligible": 80,
    "active_revisions_age_ineligible": 20,
    "active_exclusion_status_conflict": 2,
    "cleared_revision_count": 200,
    "application_date_usable": 150,
    "closed_revision_count": 100,
    "closed_matched_three_date_count": 75,
    "closed_excluded_conflicting_date_count": 5,
    "closed_excluded_missing_date_count": 15,
    "closed_excluded_out_of_order_date_count": 5,
    "closed_both_dates_available": 80,
}


class FakePrivateKey:
    def private_bytes(
        self, encoding: object, format: object, encryption_algorithm: object
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
    def __init__(
        self,
        relations: set[str],
        refresh_timestamp: datetime | None = None,
        source_extraction_timestamp: datetime | None = None,
        metadata_columns: set[str] | None = None,
    ) -> None:
        self.relations = relations
        self.refresh_timestamp = refresh_timestamp
        self.source_extraction_timestamp = source_extraction_timestamp
        self.metadata_columns = metadata_columns or {
            "LAST_SUCCESSFUL_REFRESH_UTC",
            "LAST_SUCCESSFUL_SOURCE_EXTRACTION_UTC",
        }
        self.description: list[tuple[str]] | None = None
        self.rows: list[tuple[Any, ...]] = []
        self.closed = False
        self.queries: list[str] = []

    def execute(self, query: str, params: object = None) -> None:
        self.queries.append(query)
        upper = query.upper()
        if "INFORMATION_SCHEMA.COLUMNS" in upper:
            self.rows = [(name,) for name in sorted(self.metadata_columns)]
        elif "INFORMATION_SCHEMA.TABLES" in upper:
            self.rows = [(name,) for name in sorted(self.relations)]
        elif "DASHBOARD_REFRESH_METADATA" in upper:
            selected = ["LAST_SUCCESSFUL_REFRESH_UTC"]
            values = [self.refresh_timestamp]
            if "LAST_SUCCESSFUL_SOURCE_EXTRACTION_UTC" in self.metadata_columns:
                selected.append("LAST_SUCCESSFUL_SOURCE_EXTRACTION_UTC")
                values.append(self.source_extraction_timestamp)
            self.description = [(name,) for name in selected]
            self.rows = (
                [tuple(values)] if any(value is not None for value in values) else []
            )
        elif "MART_ACTIVE_PERMIT_AGE" in upper:
            self.description = [("PERMIT_NUM",), ("IS_AGE_ELIGIBLE",), ("AS_OF_DATE",)]
            self.rows = [("00123", True, "2026-11-19")]
        elif "MART_ACTIVE_BACKLOG_BY_YEAR_TYPE" in upper:
            self.description = [
                ("APPLICATION_YEAR",),
                ("PERMIT_TYPE",),
                ("OVER_365_DAYS_COUNT",),
            ]
            self.rows = [(2025, "Residential", 7)]
        elif "MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE" in upper:
            self.description = [("APPLICATION_YEAR",), ("PERMIT_TYPE",), ("MEASURE",)]
            self.rows = [(2025, "Residential", "Application to issue")]
        elif "MART_ACTIVE_BACKLOG_SUMMARY" in upper:
            self.description = [
                ("SUMMARY_LEVEL",),
                ("APPLICATION_YEAR",),
                ("PERMIT_TYPE",),
            ]
            self.rows = [
                ("overall", None, None),
                ("application_year", 2025, None),
                ("permit_type", None, "Residential"),
            ]
        elif "MART_CLEARED_LIFECYCLE_MATCHED_BY_YEAR_TYPE" in upper:
            self.description = [
                ("APPLICATION_YEAR",),
                ("PERMIT_TYPE",),
                ("MEASURE",),
                ("MATCHED_COHORT_COUNT",),
                ("MEDIAN_DAYS",),
                ("P90_DAYS",),
            ]
            self.rows = [(2025, "Residential", "Application to issue", 25, 28.0, 129.0)]
        elif "MART_CLEARED_LIFECYCLE_POOLED" in upper:
            self.description = [
                ("MEASURE",),
                ("MATCHED_COHORT_COUNT",),
                ("MEDIAN_DAYS",),
                ("P90_DAYS",),
            ]
            self.rows = [
                ("Application to issue", 25, 28.0, 129.0),
                ("Issue to completion", 25, 292.0, 1054.0),
                ("Application to completion", 25, 345.0, 1163.0),
            ]
        elif "MART_DATA_QUALITY_SUMMARY" in upper:
            self.description = [("METRIC_NAME",), ("METRIC_VALUE",)]
            self.rows = [
                (name, QUALITY_METRIC_VALUES.get(name, 0))
                for name in backend.REQUIRED_QUALITY_METRICS
            ]
        else:
            raise AssertionError(f"Unexpected SQL query: {query}")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def close(self) -> None:
        self.closed = True


def install_snowflake_stubs(
    monkeypatch: pytest.MonkeyPatch, connection: FakeConnection
) -> dict[str, Any]:
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
    for name, module in {
        "cryptography": cryptography,
        "cryptography.hazmat": hazmat,
        "cryptography.hazmat.primitives": primitives,
        "cryptography.hazmat.primitives.serialization": serialization,
        "snowflake": snowflake,
        "snowflake.connector": connector,
    }.items():
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
    connection = FakeConnection(FakeCursor({"MART_ACTIVE_PERMIT_AGE"}))
    install_snowflake_stubs(monkeypatch, connection)
    with pytest.raises(RuntimeError, match="missing required dashboard relations"):
        backend.load_snowflake_dashboard_data(CONFIG)
    assert connection.fake_cursor.closed
    assert connection.closed


def test_read_uses_only_dbt_marts_and_closes_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refresh_timestamp = datetime(2026, 10, 28, 20, 0, tzinfo=UTC)
    source_extraction_timestamp = datetime(2026, 10, 28, 19, 58, tzinfo=UTC)
    cursor = FakeCursor(
        set(backend.REQUIRED_RELATIONS) | {"DASHBOARD_REFRESH_METADATA"},
        refresh_timestamp,
        source_extraction_timestamp,
    )
    connection = FakeConnection(cursor)
    connection_args = install_snowflake_stubs(monkeypatch, connection)
    (
        active,
        summary,
        backlog,
        backlog_by_year_type,
        matched,
        pooled,
        cleared_count,
        quality,
        actual_refresh,
        actual_source_extraction,
    ) = backend.load_snowflake_dashboard_data(CONFIG)
    assert active.loc[0, "permit_num"] == "00123"
    assert summary.loc[0, "permit_type"] == "Residential"
    assert backlog.loc[0, "summary_level"] == "overall"
    assert backlog_by_year_type.loc[0, "over_365_days_count"] == 7
    assert matched.loc[0, "matched_cohort_count"] == 25
    assert pooled.loc[0, "median_days"] == 28.0
    assert cleared_count == 200
    assert actual_refresh == refresh_timestamp
    assert actual_source_extraction == source_extraction_timestamp
    assert set(backend.REQUIRED_RELATIONS) == {
        "MART_ACTIVE_PERMIT_AGE",
        "MART_ACTIVE_BACKLOG_SUMMARY",
        "MART_ACTIVE_BACKLOG_BY_YEAR_TYPE",
        "MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE",
        "MART_CLEARED_LIFECYCLE_MATCHED_BY_YEAR_TYPE",
        "MART_CLEARED_LIFECYCLE_POOLED",
        "MART_DATA_QUALITY_SUMMARY",
    }
    assert set(backend.REQUIRED_QUALITY_METRICS).issubset(quality)
    assert quality["active_revisions_analysed"] == 100
    assert quality["active_revisions_age_eligible"] == 80
    assert quality["active_exclusion_status_conflict"] == 2
    assert quality["cleared_revision_count"] == 200
    assert quality["application_date_usable"] == 150
    assert quality["closed_both_dates_available"] == 80
    assert quality["closed_revision_count"] == 100
    assert quality["closed_matched_three_date_count"] == 75
    assert (
        quality["closed_matched_three_date_count"]
        + quality["closed_excluded_conflicting_date_count"]
        + quality["closed_excluded_missing_date_count"]
        + quality["closed_excluded_out_of_order_date_count"]
        == quality["closed_revision_count"]
    )
    assert not any(
        "FCT_ACTIVE_PERMIT_REVISION" in query or "FCT_CLEARED_PERMIT_REVISION" in query
        for query in cursor.queries
    )
    assert not any(
        "MEDIAN(" in query.upper() or "PERCENTILE_CONT" in query.upper()
        for query in cursor.queries
    )
    assert connection_args["role"] == "TORONTO_DASHBOARD_ROLE"
    assert connection_args["warehouse"] == "TORONTO_XS_WH"
    assert connection_args["database"] == "TORONTO"
    assert cursor.closed and connection.closed


def test_refresh_timestamp_is_optional_before_first_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeConnection(FakeCursor(set(backend.REQUIRED_RELATIONS)))
    install_snowflake_stubs(monkeypatch, connection)
    result = backend.load_snowflake_dashboard_data(CONFIG)
    assert result[8] is None
    assert result[9] is None
    assert connection.fake_cursor.closed and connection.closed


def test_old_snowflake_metadata_schema_keeps_refresh_and_omits_source_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refresh_timestamp = datetime(2026, 10, 28, 20, 0, tzinfo=UTC)
    cursor = FakeCursor(
        set(backend.REQUIRED_RELATIONS) | {"DASHBOARD_REFRESH_METADATA"},
        refresh_timestamp,
        metadata_columns={"LAST_SUCCESSFUL_REFRESH_UTC"},
    )
    connection = FakeConnection(cursor)
    install_snowflake_stubs(monkeypatch, connection)

    result = backend.load_snowflake_dashboard_data(CONFIG)
    assert result[8] == refresh_timestamp
    assert result[9] is None
    assert connection.fake_cursor.closed and connection.closed
