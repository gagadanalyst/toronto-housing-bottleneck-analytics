"""Small filesystem, logging, Parquet, and quality-check helpers."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

LOGGER = logging.getLogger("toronto_housing_ingestion")


def log_event(level: int, event: str, **fields: Any) -> None:
    """Write one structured JSON event to the application log."""
    payload = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "event": event,
        **fields,
    }
    LOGGER.log(level, json.dumps(payload, default=str, sort_keys=True))


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON atomically so a crash cannot leave partial metadata."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as file:
        temp_path = Path(file.name)
        json.dump(payload, file, indent=2, default=str, sort_keys=True)
        file.write("\n")
    try:
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def atomic_write_parquet(dataframe: pd.DataFrame, path: Path) -> None:
    """Write one DataFrame to Parquet without storing a pandas index."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        suffix=".parquet", dir=path.parent, delete=False
    ) as file:
        temp_path = Path(file.name)
    try:
        dataframe.to_parquet(temp_path, index=False)
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def load_json(path: Path) -> dict[str, Any] | None:
    """Load JSON or return ``None`` when the file does not exist."""
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON checkpoint: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return value


def checkpoint_matches(
    checkpoint: dict[str, Any], resource_id: str, fingerprint: dict[str, Any]
) -> bool:
    """Return whether a checkpoint belongs to the same source snapshot."""
    return (
        checkpoint.get("resource_id") == resource_id
        and checkpoint.get("fingerprint") == fingerprint
    )


def combine_parquet_parts(parts: list[Path], destination: Path) -> None:
    """Combine page Parquet files one at a time into one final Parquet file."""
    if not parts:
        raise ValueError("No Parquet parts were produced.")

    import pyarrow as pa
    from pyarrow import parquet as pq

    destination.parent.mkdir(parents=True, exist_ok=True)
    schemas = [pq.read_schema(part) for part in parts]
    schema = pa.unify_schemas(schemas, promote_options="permissive")

    with tempfile.NamedTemporaryFile(
        suffix=".parquet", dir=destination.parent, delete=False
    ) as file:
        temp_path = Path(file.name)

    writer = pq.ParquetWriter(temp_path, schema)
    try:
        for part in parts:
            writer.write_table(pq.read_table(part).cast(schema))
        writer.close()
        writer = None
        os.replace(temp_path, destination)
    finally:
        if writer is not None:
            writer.close()
        temp_path.unlink(missing_ok=True)


def parquet_row_count(path: Path) -> int:
    """Return the number of rows in a Parquet file without loading its data."""
    from pyarrow import parquet as pq

    return int(pq.ParquetFile(path).metadata.num_rows)


def actual_columns(path: Path) -> list[str]:
    """Return Parquet column names."""
    from pyarrow import parquet as pq

    return list(pq.ParquetFile(path).schema_arrow.names)


def validate_required_columns(
    dataframe: pd.DataFrame, required_columns: tuple[str, ...], source_name: str
) -> None:
    """Raise when configured required columns are missing."""
    actual = {column.lower() for column in dataframe.columns}
    missing = [column for column in required_columns if column.lower() not in actual]
    if missing:
        raise ValueError(f"{source_name}: required columns are missing: {missing}")


def validate_data_types(
    dataframe: pd.DataFrame, date_columns: tuple[str, ...], source_name: str
) -> None:
    """Check configured date columns without rejecting null values."""
    actual = {column.lower(): column for column in dataframe.columns}
    for configured in date_columns:
        column = actual.get(configured.lower())
        if not column:
            continue
        values = dataframe.loc[dataframe[column].notna(), column]
        parsed = pd.to_datetime(values, errors="coerce", format="mixed")
        if parsed.isna().any():
            raise ValueError(f"{source_name}: {column} contains unparseable dates.")


def key_quality_warnings(
    parquet_path: Path,
    key_columns: tuple[str, ...],
    source_name: str,
) -> list[dict[str, Any]]:
    """Check configured natural keys and return warnings without failing extraction.

    A bad natural key is a data-quality problem, not a reason to discard the raw
    source snapshot. Callers can use the returned warnings to block an unsafe
    warehouse merge while keeping the Parquet file.
    """
    if not key_columns:
        return []

    import duckdb

    columns = {column.lower(): column for column in actual_columns(parquet_path)}
    missing = [column for column in key_columns if column.lower() not in columns]
    if missing:
        return [
            {
                "type": "missing_key_columns",
                "source_name": source_name,
                "columns": missing,
            }
        ]

    actual_keys = [columns[column.lower()] for column in key_columns]
    quoted_keys = ", ".join(_quote_identifier(column) for column in actual_keys)

    with duckdb.connect() as connection:
        missing_values = int(
            connection.execute(
                "SELECT COUNT(*) FROM read_parquet(?) WHERE "
                + " OR ".join(f"{_quote_identifier(c)} IS NULL" for c in actual_keys),
                [str(parquet_path)],
            ).fetchone()[0]
        )

        duplicate_rows = int(
            connection.execute(
                "SELECT COALESCE(SUM(n - 1), 0) FROM ("
                f"SELECT {quoted_keys}, COUNT(*) AS n "
                "FROM read_parquet(?) "
                f"GROUP BY {quoted_keys} HAVING COUNT(*) > 1"
                ")",
                [str(parquet_path)],
            ).fetchone()[0]
        )

    warnings: list[dict[str, Any]] = []
    if missing_values:
        warnings.append(
            {
                "type": "missing_key",
                "source_name": source_name,
                "rows": missing_values,
                "primary_key": list(key_columns),
            }
        )
    if duplicate_rows:
        warnings.append(
            {
                "type": "duplicate_key",
                "source_name": source_name,
                "rows": duplicate_rows,
                "primary_key": list(key_columns),
            }
        )
    return warnings


def _quote_identifier(name: str) -> str:
    """Return a safely quoted SQL identifier."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{name}"'


def sha256_file(path: Path) -> str:
    """Return a SHA-256 digest using bounded memory."""
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def exclusive_lock(path: Path) -> Iterator[None]:
    """Prevent two ingestion jobs from writing the same local state at once."""
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        fcntl.flock(file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(file.fileno(), fcntl.LOCK_UN)
