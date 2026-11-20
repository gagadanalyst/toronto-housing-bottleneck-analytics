"""Format backend refresh timestamps consistently for the dashboard."""

from __future__ import annotations

from typing import Any

import pandas as pd


def format_refresh_timestamp(value: Any) -> str:
    """Convert a UTC timestamp to Toronto local time (ET) for display."""
    if value is None:
        return "Not yet recorded"

    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return "Not yet recorded"

    return timestamp.tz_convert("America/Toronto").strftime("%d %b %Y · %I:%M %p")


def latest_source_extraction_timestamp(
    connection: Any, available_tables: set[tuple[str, str]]
) -> Any | None:
    """Return the latest source extraction time represented by a complete loaded pair.

    V2 archives provide a true per-source completion timestamp. Older ledgers
    fall back to their recorded extraction-start boundary. Partial or failed
    Active/Cleared loads are never presented as the latest loaded snapshot.
    """
    normalized_tables = {
        (str(schema).lower(), str(table).lower()) for schema, table in available_tables
    }
    if ("raw", "toronto_snapshot_ledger") not in normalized_tables:
        return None

    columns = {
        str(row[0]).lower()
        for row in connection.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?",
            ["raw", "toronto_snapshot_ledger"],
        ).fetchall()
    }
    if "extraction_started_at_utc" not in columns:
        return None

    timestamp_expression = (
        "COALESCE(extraction_completed_at_utc, extraction_started_at_utc)"
        if "extraction_completed_at_utc" in columns
        else "extraction_started_at_utc"
    )
    row = connection.execute(
        f"""SELECT MAX({timestamp_expression})
            FROM raw.toronto_snapshot_ledger
            WHERE LOWER(source_name) IN ('active_permits', 'cleared_permits')
              AND snapshot_id IN (
                  SELECT snapshot_id
                  FROM raw.toronto_snapshot_ledger
                  WHERE LOWER(source_name) IN ('active_permits', 'cleared_permits')
                  GROUP BY snapshot_id
                  HAVING COUNT(DISTINCT LOWER(source_name)) = 2
                     AND COUNT(*) = 2
                     AND COUNT_IF(LOWER(status) = 'succeeded') = 2
              )
        """
    ).fetchone()
    return row[0] if row and row[0] is not None else None
