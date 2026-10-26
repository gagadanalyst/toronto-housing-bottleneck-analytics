"""Load full Parquet snapshots into the configured dlt warehouse strategy.

Extraction is intentionally separate from warehouse loading. The extracted
Parquet file keeps the complete source snapshot; the optional warehouse date
filter is applied only while reading rows for dlt.

The existing Active/Cleared permit resources use SCD2. Supporting resources can
use full-snapshot replacement by setting ``warehouse_strategy: replace``.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import SourceConfig


@dataclass(frozen=True)
class LoadResult:
    """Summary returned after a successful dlt load."""

    table_name: str
    destination_kind: str
    total_rows: int | None
    current_rows: int | None
    load_info: Any


def _destination(kind: str, credentials: str) -> Any:
    """Create the configured dlt destination."""
    if kind == "duckdb":
        from dlt.destinations import duckdb

        return duckdb(credentials=credentials)
    if kind == "snowflake":
        from dlt.destinations import snowflake

        # Keep dlt's staging schema aligned with the migrated TIMESTAMP_LTZ contract.
        # This option is supported by the project's new dlt 1.31.0 pin.
        return snowflake(credentials=credentials, use_timestamp_tz=False)
    raise ValueError(f"Unsupported warehouse destination: {kind}")


def _identifier(value: str, label: str) -> str:
    """Validate a simple SQL identifier used for DuckDB queries."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def ensure_duckdb_database(credentials: str, dataset_name: str) -> None:
    """Create the DuckDB parent directory and raw schema."""
    import duckdb

    db_path = Path(credentials)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    dataset = _identifier(dataset_name, "dataset name").lower()
    with duckdb.connect(credentials) as connection:
        connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{dataset}"')


def _record_in_warehouse_window(
    row: dict[str, Any],
    date_column: str | None,
    since: str | None,
) -> bool:
    """Return True when a row belongs in the warehouse date window."""
    if not date_column or not since:
        return True
    value = row.get(date_column)
    return value is not None and str(value)[:10] >= since


def _iter_snapshot_rows(
    path: Path,
    date_column: str | None,
    since: str | None,
) -> Iterator[dict[str, Any]]:
    """Yield rows from a Parquet snapshot, optionally filtered by date."""
    from pyarrow import parquet as pq

    parquet = pq.ParquetFile(path)
    actual_column = None
    if date_column:
        actual_column = next(
            (c for c in parquet.schema_arrow.names if c.lower() == date_column.lower()),
            None,
        )
        if actual_column is None:
            raise ValueError(f"Warehouse date column not found: {date_column}")

    for batch in parquet.iter_batches(batch_size=1000):
        for row in batch.to_pylist():
            if not _record_in_warehouse_window(row, actual_column, since):
                continue
            yield row


def _row_hash(row: dict[str, Any]) -> str:
    """Return a stable hash of source attributes, excluding the unstable _id."""
    tracked = {key: value for key, value in row.items() if key != "_id"}
    payload = json.dumps(
        tracked,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def load_scd2(
    *,
    credentials: str,
    snapshot_path: str | Path,
    source: SourceConfig,
    dataset_name: str,
    destination_kind: str,
    preserve_row_hash: bool = False,
) -> LoadResult:
    """Load a full Parquet snapshot using ``source.warehouse_strategy``.

    The function name is retained so existing ingestion call sites continue to
    work. For ``scd2``, the configured primary key and row hash maintain history.
    For ``replace``, the destination table is replaced by the latest snapshot,
    without applying a dlt primary-key hint.

    ``preserve_row_hash`` is an opt-in for controlled continuity fixtures that
    carry stored hashes from the migrated warehouse. Normal ingestion leaves it
    disabled and computes hashes from source snapshot attributes as before.

    If the source config has ``warehouse_since`` and ``warehouse_date_column``,
    only rows on or after that date are sent to the warehouse.
    """
    destination_kind = destination_kind.lower()
    if not source.warehouse_load:
        raise ValueError(f"{source.name}: warehouse loading is disabled.")

    strategy = source.warehouse_strategy.lower()
    if strategy not in {"scd2", "replace"}:
        raise ValueError(
            f"{source.name}: unsupported warehouse strategy: {strategy!r}."
        )
    if strategy == "scd2" and not source.primary_key:
        raise ValueError(f"{source.name}: primary_key is required for SCD2.")
    if preserve_row_hash and strategy != "scd2":
        raise ValueError("preserve_row_hash is supported only for SCD2 snapshots.")

    path = Path(snapshot_path)
    if not path.is_file():
        raise FileNotFoundError(path)

    table_name = _identifier(source.warehouse_table, "warehouse table")
    dataset = _identifier(dataset_name, "dataset name")

    import dlt

    @dlt.resource(name=table_name)
    def rows() -> Iterator[dict[str, Any]]:
        """Yield warehouse rows from the complete Parquet snapshot."""
        for row in _iter_snapshot_rows(
            path, source.warehouse_date_column, source.warehouse_since
        ):
            if strategy == "scd2":
                if preserve_row_hash:
                    hash_key = next(
                        (key for key in row if key.lower() == "row_hash"), None
                    )
                    supplied_hash = row.get(hash_key) if hash_key is not None else None
                    if not isinstance(supplied_hash, str) or not supplied_hash:
                        raise ValueError(
                            f"{source.name}: preserve_row_hash=True requires a non-empty "
                            "string row_hash in every snapshot row."
                        )
                    if hash_key != "row_hash":
                        row.pop(hash_key)
                        row["row_hash"] = supplied_hash
                else:
                    row["row_hash"] = _row_hash(row)
            yield row

    resource = rows()
    if strategy == "scd2":
        resource.apply_hints(
            primary_key=list(source.primary_key),
            write_disposition={
                "disposition": "merge",
                "strategy": "scd2",
                "row_version_column_name": "row_hash",
            },
            columns=source.type_hints,
        )
    else:
        resource.apply_hints(
            write_disposition="replace",
            columns=source.type_hints,
        )

    if destination_kind == "duckdb":
        credentials = str((Path(__file__).resolve().parents[2] / credentials).resolve())

    pipeline = dlt.pipeline(
        # Preserve the existing pipeline names for Active/Cleared SCD2. Using
        # a separate name for replacement avoids reusing a failed SCD2 package.
        pipeline_name=f"toronto_{source.name}_{strategy}",
        destination=_destination(destination_kind, credentials),
        dataset_name=dataset,
        dev_mode=False,
        progress="log",
    )
    info = pipeline.run(resource)
    info.raise_on_failed_jobs()

    total_rows: int | None = None
    current_rows: int | None = None
    if destination_kind == "duckdb":
        import duckdb

        qualified = f'"{dataset}"."{table_name}"'
        with duckdb.connect(credentials, read_only=True) as connection:
            total_rows = int(
                connection.execute(f"SELECT COUNT(*) FROM {qualified}").fetchone()[0]
            )
            if strategy == "scd2":
                current_rows = int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM {qualified} WHERE _dlt_valid_to IS NULL"
                    ).fetchone()[0]
                )
            else:
                # Replacement tables have no SCD2 current-version semantics.
                current_rows = total_rows

    return LoadResult(
        table_name=table_name,
        destination_kind=destination_kind,
        total_rows=total_rows,
        current_rows=current_rows,
        load_info=info,
    )
