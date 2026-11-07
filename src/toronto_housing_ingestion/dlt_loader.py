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
    quarantined_business_keys: tuple[dict[str, Any], ...] = ()
    quarantined_row_count: int = 0


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


_HASH_FLOAT_FIELDS = frozenset(
    {
        "ASSEMBLY",
        "BUSINESS_AND_PERSONAL_SERVICES",
        "DEMOLITION",
        "INDUSTRIAL",
        "INSTITUTIONAL",
        "INTERIOR_ALTERATIONS",
        "MERCANTILE",
        "RESIDENTIAL",
    }
)


def _row_hash(row: dict[str, Any]) -> str:
    """Hash source attributes using legacy-compatible numeric normalization."""
    tracked = {}

    for key, value in row.items():
        if key == "_id":
            continue

        if (
            key.upper() in _HASH_FLOAT_FIELDS
            and value is not None
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
        ):
            value = float(value)

        tracked[key] = value

    payload = json.dumps(
        tracked,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _resolve_scd2_key_columns(
    snapshot_path: Path,
    source: SourceConfig,
) -> tuple[str, ...]:
    """Resolve configured business-key columns against the Parquet schema."""
    from pyarrow import parquet as pq

    columns = {
        column.lower(): column
        for column in pq.ParquetFile(snapshot_path).schema_arrow.names
    }
    missing = [column for column in source.primary_key if column.lower() not in columns]
    if missing:
        raise ValueError(
            f"{source.name}: SCD2 business-key columns missing from snapshot: {missing}"
        )
    return tuple(columns[column.lower()] for column in source.primary_key)


def _scd2_row_hash(
    row: dict[str, Any],
    *,
    preserve_row_hash: bool,
    source_name: str,
) -> str:
    """Return the hash that dlt will use to identify an SCD2 row version."""
    if not preserve_row_hash:
        return _row_hash(row)

    hash_key = next(
        (key for key in row if key.lower() == "row_hash"),
        None,
    )
    supplied_hash = row.get(hash_key) if hash_key is not None else None
    if not isinstance(supplied_hash, str) or not supplied_hash:
        raise ValueError(
            f"{source_name}: preserve_row_hash=True requires a non-empty "
            "string row_hash in every snapshot row."
        )
    return supplied_hash


def _validate_scd2_snapshot(
    snapshot_path: Path,
    *,
    source: SourceConfig,
    key_columns: tuple[str, ...],
    preserve_row_hash: bool,
) -> tuple[list[tuple[tuple[Any, ...], Any, str, Any, str]], int]:
    """Find conflicting SCD2 keys and reject incomplete business keys.

    Identical duplicate records are safe. Conflicting keys are quarantined
    while the rest of the snapshot can continue through the SCD2 load.
    """
    seen: dict[tuple[Any, ...], tuple[str, Any, int]] = {}
    conflicts: list[tuple[tuple[Any, ...], Any, str, Any, str]] = []
    conflicting_keys: set[tuple[Any, ...]] = set()
    quarantined_row_count = 0
    invalid_key_rows = 0
    invalid_key_examples: list[tuple[Any, list[str]]] = []

    id_column = next(
        (
            column
            for column in _parquet_column_names(snapshot_path)
            if column.lower() == "_id"
        ),
        None,
    )

    for row in _iter_snapshot_rows(
        snapshot_path,
        source.warehouse_date_column,
        source.warehouse_since,
    ):
        key = tuple(row.get(column) for column in key_columns)
        missing = [
            configured
            for configured, value in zip(source.primary_key, key, strict=True)
            if value is None or (isinstance(value, str) and not value.strip())
        ]
        source_id = row.get(id_column) if id_column else None

        if missing:
            invalid_key_rows += 1
            if len(invalid_key_examples) < 5:
                invalid_key_examples.append((source_id, missing))
            continue

        row_hash = _scd2_row_hash(
            row,
            preserve_row_hash=preserve_row_hash,
            source_name=source.name,
        )
        previous = seen.get(key)

        if previous is None:
            seen[key] = (row_hash, source_id, 1)
            continue

        row_count = previous[2] + 1
        seen[key] = (previous[0], previous[1], row_count)

        if key in conflicting_keys:
            quarantined_row_count += 1
        elif previous[0] != row_hash:
            conflicts.append((key, previous[1], previous[0], source_id, row_hash))
            conflicting_keys.add(key)
            quarantined_row_count += row_count

    if invalid_key_rows:
        details = [
            f"{source.name}: {invalid_key_rows} row(s) have null or blank "
            "SCD2 business-key values. Warehouse load was not started."
        ]
        for source_id, missing in invalid_key_examples:
            details.append(f"  _id={source_id!r}: missing {missing}")
        raise ValueError("\n".join(details))

    return conflicts, quarantined_row_count


def _parquet_column_names(snapshot_path: Path) -> list[str]:
    """Return the columns in a Parquet snapshot."""
    from pyarrow import parquet as pq

    return pq.ParquetFile(snapshot_path).schema_arrow.names


def _destination_scope_excluding_keys(
    key_columns: tuple[str, ...],
    quarantined_keys: set[tuple[Any, ...]],
) -> str | None:
    """Prevent dlt from retiring existing records for quarantined keys."""
    if not quarantined_keys:
        return None

    predicates = []
    for key in sorted(
        quarantined_keys,
        key=lambda values: tuple(str(value) for value in values),
    ):
        conditions = []
        for column, value in zip(key_columns, key, strict=True):
            identifier = _identifier(column.lower(), "SCD2 key column")
            if isinstance(value, bool):
                literal = "TRUE" if value else "FALSE"
            elif isinstance(value, (int, float)):
                literal = str(value)
            else:
                literal = "'" + str(value).replace("'", "''") + "'"
            conditions.append(f'"{identifier}" = {literal}')
        predicates.append("(" + " AND ".join(conditions) + ")")

    return "NOT (" + " OR ".join(predicates) + ")"


def _iter_scd2_snapshot_rows(
    snapshot_path: Path,
    *,
    key_columns: tuple[str, ...],
    date_column: str | None,
    since: str | None,
    preserve_row_hash: bool,
    source_name: str,
    quarantined_keys: set[tuple[Any, ...]],
) -> Iterator[dict[str, Any]]:
    """Yield one row per valid business key, excluding quarantined keys."""
    seen_keys: set[tuple[Any, ...]] = set()

    for row in _iter_snapshot_rows(snapshot_path, date_column, since):
        key = tuple(row.get(column) for column in key_columns)

        if key in quarantined_keys or key in seen_keys:
            continue

        seen_keys.add(key)

        row_hash = _scd2_row_hash(
            row,
            preserve_row_hash=preserve_row_hash,
            source_name=source_name,
        )
        if preserve_row_hash:
            hash_key = next(key for key in row if key.lower() == "row_hash")
            if hash_key != "row_hash":
                row.pop(hash_key)
        row["row_hash"] = row_hash
        yield row


def load_scd2(
    *,
    credentials: str,
    snapshot_path: str | Path,
    source: SourceConfig,
    dataset_name: str,
    destination_kind: str,
    preserve_row_hash: bool = False,
    pipeline_name: str | None = None,
    boundary_timestamp: str | None = None,
) -> LoadResult:
    """Load a full Parquet snapshot using ``source.warehouse_strategy``.

    The function name is retained so existing ingestion call sites continue to
    work. For ``scd2``, the configured primary key and row hash maintain history.
    For ``replace``, the destination table is replaced by the latest snapshot,
    without applying a dlt primary-key hint.

    ``preserve_row_hash`` is an opt-in for controlled continuity fixtures that
    carry stored hashes from the migrated warehouse. Normal ingestion leaves it
    disabled and computes hashes from source snapshot attributes as before.

    ``boundary_timestamp`` optionally sets the historical SCD2 validity boundary
    for controlled snapshot replay. Pass an ISO-8601 timestamp with timezone.

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

    key_columns: tuple[str, ...] | None = None
    quarantined_key_values: set[tuple[Any, ...]] = set()
    quarantined_details: tuple[dict[str, Any], ...] = ()
    quarantined_row_count = 0
    destination_scope: str | None = None

    if strategy == "scd2":
        key_columns = _resolve_scd2_key_columns(path, source)
        conflicts, quarantined_row_count = _validate_scd2_snapshot(
            path,
            source=source,
            key_columns=key_columns,
            preserve_row_hash=preserve_row_hash,
        )
        quarantined_key_values = {conflict[0] for conflict in conflicts}

        quarantined_details = tuple(
            {
                "business_key": dict(zip(source.primary_key, key, strict=True)),
                "source_ids": [first_id, second_id],
                "row_hash_prefixes": [
                    first_hash[:12],
                    second_hash[:12],
                ],
            }
            for key, first_id, first_hash, second_id, second_hash in conflicts
        )
        destination_scope = _destination_scope_excluding_keys(
            source.primary_key,
            quarantined_key_values,
        )

    import dlt

    @dlt.resource(name=table_name)
    def rows() -> Iterator[dict[str, Any]]:
        """Yield warehouse rows from the complete Parquet snapshot."""
        if strategy == "scd2":
            assert key_columns is not None
            yield from _iter_scd2_snapshot_rows(
                path,
                key_columns=key_columns,
                date_column=source.warehouse_date_column,
                since=source.warehouse_since,
                preserve_row_hash=preserve_row_hash,
                source_name=source.name,
                quarantined_keys=quarantined_key_values,
            )
        else:
            yield from _iter_snapshot_rows(
                path, source.warehouse_date_column, source.warehouse_since
            )

    resource = rows()
    if strategy == "scd2":
        write_disposition = {
            "disposition": "merge",
            "strategy": "scd2",
            "row_version_column_name": "row_hash",
        }
        if boundary_timestamp is not None:
            write_disposition["boundary_timestamp"] = boundary_timestamp
        if destination_scope:
            write_disposition["destination_scope"] = destination_scope

        resource.apply_hints(
            primary_key=list(source.primary_key),
            write_disposition=write_disposition,
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
        pipeline_name=pipeline_name or f"toronto_{source.name}_{strategy}",
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
        quarantined_business_keys=quarantined_details,
        quarantined_row_count=quarantined_row_count,
    )
