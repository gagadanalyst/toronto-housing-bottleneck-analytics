"""Small filesystem, logging and Parquet helpers for extraction."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

LOGGER = logging.getLogger("toronto_housing_extraction")


def log_event(level: int, event: str, **fields: Any) -> None:
    """Write a structured JSON event to the application log."""
    payload = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "event": event,
        **fields,
    }
    LOGGER.log(level, json.dumps(payload, default=str, sort_keys=True))


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON atomically to prevent partially written metadata."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
    ) as file:
        temp_path = Path(file.name)
        json.dump(payload, file, indent=2, default=str, sort_keys=True)
        file.write("\n")

    try:
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def atomic_write_parquet(dataframe: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to Parquet atomically, without its index."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        suffix=".parquet",
        dir=path.parent,
        delete=False,
    ) as file:
        temp_path = Path(file.name)

    try:
        dataframe.to_parquet(temp_path, index=False)
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def load_json(path: Path) -> dict[str, Any] | None:
    """Load a JSON object, returning None when the file does not exist."""
    if not path.exists():
        return None

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON checkpoint: {path}") from exc

    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}")

    return value


def checkpoint_matches(
    checkpoint: dict[str, Any],
    resource_id: str,
    fingerprint: dict[str, Any],
) -> bool:
    """Check whether a checkpoint belongs to the same source version."""
    return (
        checkpoint.get("resource_id") == resource_id
        and checkpoint.get("fingerprint") == fingerprint
    )


def combine_parquet_parts(parts: list[Path], destination: Path) -> None:
    """Combine Parquet parts one at a time into a single file."""
    if not parts:
        raise ValueError("No Parquet parts were produced.")

    import pyarrow as pa
    from pyarrow import parquet as pq

    destination.parent.mkdir(parents=True, exist_ok=True)

    schemas = [pq.read_schema(part) for part in parts]
    schema = pa.unify_schemas(schemas, promote_options="permissive")

    with tempfile.NamedTemporaryFile(
        suffix=".parquet",
        dir=destination.parent,
        delete=False,
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
    """Read the Parquet metadata row count without loading the data."""
    from pyarrow import parquet as pq

    return int(pq.ParquetFile(path).metadata.num_rows)


def actual_columns(path: Path) -> list[str]:
    """Return the column names recorded in a Parquet file."""
    from pyarrow import parquet as pq

    return list(pq.ParquetFile(path).schema_arrow.names)


def validate_required_columns(
    dataframe: pd.DataFrame,
    required_columns: tuple[str, ...],
    source_name: str,
) -> None:
    """Raise when required source columns are missing."""
    actual = {str(column).lower() for column in dataframe.columns}
    missing = [column for column in required_columns if column.lower() not in actual]

    if missing:
        raise ValueError(f"{source_name}: required columns are missing: {missing}")


def sha256_file(path: Path) -> str:
    """Calculate a file's SHA-256 checksum using bounded memory."""
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()
