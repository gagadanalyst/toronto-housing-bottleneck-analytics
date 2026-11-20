from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import yaml

from dashboard.refresh_metadata import latest_source_extraction_timestamp

LEDGER_TABLE = {("raw", "toronto_snapshot_ledger")}


def _create_ledger(
    connection: duckdb.DuckDBPyConnection, *, with_completion: bool = True
) -> None:
    completion_column = (
        "extraction_completed_at_utc TIMESTAMPTZ," if with_completion else ""
    )
    connection.execute("CREATE SCHEMA raw")
    connection.execute(
        f"""CREATE TABLE raw.toronto_snapshot_ledger (
            snapshot_id VARCHAR NOT NULL,
            source_name VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            extraction_started_at_utc TIMESTAMPTZ NOT NULL,
            {completion_column}
            PRIMARY KEY (snapshot_id, source_name)
        )"""
    )


def test_latest_timestamp_uses_latest_fully_succeeded_active_cleared_pair() -> None:
    connection = duckdb.connect(":memory:")
    _create_ledger(connection)
    old_start = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
    old_end = datetime(2026, 11, 18, 12, 12, tzinfo=UTC)
    newer_start = datetime(2026, 11, 19, 12, 0, tzinfo=UTC)
    newer_end = datetime(2026, 11, 19, 12, 15, tzinfo=UTC)
    partial_start = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    partial_end = datetime(2026, 11, 20, 12, 20, tzinfo=UTC)
    connection.executemany(
        """INSERT INTO raw.toronto_snapshot_ledger VALUES (?, ?, ?, ?, ?)""",
        [
            ("old", "active_permits", "succeeded", old_start, old_end),
            ("old", "cleared_permits", "succeeded", old_start, old_end),
            ("newer", "active_permits", "succeeded", newer_start, newer_end),
            ("newer", "cleared_permits", "succeeded", newer_start, newer_end),
            ("partial", "active_permits", "succeeded", partial_start, partial_end),
            ("partial", "cleared_permits", "failed", partial_start, partial_end),
        ],
    )

    result = latest_source_extraction_timestamp(connection, LEDGER_TABLE)
    assert result == newer_end
    connection.close()


def test_legacy_ledger_falls_back_to_recorded_extraction_start() -> None:
    connection = duckdb.connect(":memory:")
    _create_ledger(connection, with_completion=False)
    started = datetime(2026, 10, 28, 20, 0, tzinfo=UTC)
    connection.executemany(
        """INSERT INTO raw.toronto_snapshot_ledger VALUES (?, ?, ?, ?)""",
        [
            ("legacy", "active_permits", "succeeded", started),
            ("legacy", "cleared_permits", "succeeded", started),
        ],
    )

    result = latest_source_extraction_timestamp(connection, LEDGER_TABLE)
    assert result == started
    connection.close()


def test_missing_ledger_returns_none() -> None:
    connection = duckdb.connect(":memory:")
    assert latest_source_extraction_timestamp(connection, set()) is None
    connection.close()


def test_snowflake_serving_workflow_records_source_and_refresh_timestamps() -> None:
    workflow_path = (
        Path(__file__).resolve().parents[1]
        / ".github/workflows/snowflake-serving-schema.yml"
    )
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["build"]["steps"]
    metadata_step = next(
        step
        for step in steps
        if step.get("name") == "Ensure dashboard refresh metadata table exists"
    )
    refresh_step = next(
        step
        for step in steps
        if step.get("name") == "Record successful dashboard refresh timestamp"
    )

    assert "LAST_SUCCESSFUL_SOURCE_EXTRACTION_UTC TIMESTAMP_TZ" in metadata_step["run"]
    assert (
        "ADD COLUMN IF NOT EXISTS LAST_SUCCESSFUL_SOURCE_EXTRACTION_UTC"
        in metadata_step["run"]
    )
    assert "TORONTO.RAW.TORONTO_SNAPSHOT_LEDGER" in refresh_step["run"]
    assert "EXTRACTION_COMPLETED_AT_UTC" in refresh_step["run"]
    assert "COUNT_IF(LOWER(STATUS) = 'succeeded') = 2" in refresh_step["run"]
    assert "LAST_SUCCESSFUL_SOURCE_EXTRACTION_UTC" in refresh_step["run"]
