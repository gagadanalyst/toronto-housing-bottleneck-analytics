"""Load full Parquet snapshots into the configured dlt warehouse strategy.

Extraction is intentionally separate from warehouse loading. The extracted
Parquet file keeps the complete source snapshot; the optional warehouse date
filter is applied only while reading rows for dlt.

The existing Active/Cleared permit resources use SCD2. Supporting resources can
use full-snapshot replacement by setting ``warehouse_strategy: replace``.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

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
    quarantined_bad_row_count: int = 0
    quarantined_business_key_count: int = 0
    quarantine_sha256: str | None = None


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


@dataclass(frozen=True)
class SCD2SnapshotAnalysis:
    """Deterministic row-level quarantine analysis for one source snapshot."""

    conflicting_keys: frozenset[tuple[Any, ...]]
    excluded_ordinals: frozenset[int]
    quarantine_rows: tuple[dict[str, Any], ...]
    conflict_details: tuple[tuple[tuple[Any, ...], Any, str, Any, str], ...]
    bad_row_count: int
    business_key_count: int
    duplicate_row_count: int
    invalid_key_row_count: int


def _iter_snapshot_rows_with_ordinals(
    path: Path,
    date_column: str | None,
    since: str | None,
) -> Iterator[tuple[dict[str, Any], int]]:
    """Yield source rows with stable physical Parquet row ordinals (zero-based)."""
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

    ordinal = 0
    for batch in parquet.iter_batches(batch_size=1000):
        for row in batch.to_pylist():
            current_ordinal = ordinal
            ordinal += 1
            if _record_in_warehouse_window(row, actual_column, since):
                yield row, current_ordinal


def _snapshot_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _analyze_scd2_snapshot(
    snapshot_path: Path,
    *,
    source: SourceConfig,
    key_columns: tuple[str, ...],
    preserve_row_hash: bool,
) -> SCD2SnapshotAnalysis:
    """Identify invalid keys, conflicting groups, and redundant identical rows.

    A first pass records only key/hash/count metadata. A second pass materializes
    full source payloads for rows that must be retained in quarantine. This avoids
    retaining every source row in memory while preserving the complete bad rows.
    """
    # key -> first hash, first ordinal, first source id, count, first different hash/id
    seen: dict[tuple[Any, ...], tuple[str, int, Any, int, str | None, Any]] = {}
    conflict_details: list[tuple[tuple[Any, ...], Any, str, Any, str]] = []
    conflicting_keys: set[tuple[Any, ...]] = set()
    id_column = next(
        (
            column
            for column in _parquet_column_names(snapshot_path)
            if column.lower() == "_id"
        ),
        None,
    )

    for row, ordinal in _iter_snapshot_rows_with_ordinals(
        snapshot_path, source.warehouse_date_column, source.warehouse_since
    ):
        key = tuple(row.get(column) for column in key_columns)
        if any(
            value is None or (isinstance(value, str) and not value.strip())
            for value in key
        ):
            continue
        row_hash = _scd2_row_hash(
            row, preserve_row_hash=preserve_row_hash, source_name=source.name
        )
        source_id = row.get(id_column) if id_column else None
        previous = seen.get(key)
        if previous is None:
            seen[key] = (row_hash, ordinal, source_id, 1, None, None)
            continue
        first_hash, first_ordinal, first_id, count, other_hash, other_id = previous
        count += 1
        if first_hash != row_hash:
            conflicting_keys.add(key)
            if other_hash is None:
                other_hash, other_id = row_hash, source_id
                conflict_details.append(
                    (key, first_id, first_hash, source_id, row_hash)
                )
        seen[key] = (first_hash, first_ordinal, first_id, count, other_hash, other_id)

    # All ordinals for conflicting groups and invalid keys are excluded. For an
    # identical duplicate group, keep its first physical occurrence and quarantine
    # each redundant occurrence for audit without counting it toward the 25% gate.
    excluded_ordinals: set[int] = set()
    quarantine_rows: list[dict[str, Any]] = []
    bad_row_count = 0
    duplicate_row_count = 0
    invalid_key_rows = 0
    conflict_key_count = len(conflicting_keys)

    for row, ordinal in _iter_snapshot_rows_with_ordinals(
        snapshot_path, source.warehouse_date_column, source.warehouse_since
    ):
        key = tuple(row.get(column) for column in key_columns)
        missing = [
            configured
            for configured, value in zip(source.primary_key, key, strict=True)
            if value is None or (isinstance(value, str) and not value.strip())
        ]
        reason: str | None = None
        if missing:
            reason = "invalid_business_key"
            business_key = None
            invalid_key_rows += 1
            row_hash = _scd2_row_hash(
                row, preserve_row_hash=preserve_row_hash, source_name=source.name
            )
        else:
            group = seen.get(key)
            if group is not None and key in conflicting_keys:
                reason = "conflicting_duplicate_business_key"
                business_key = dict(zip(source.primary_key, key, strict=True))
                row_hash = _scd2_row_hash(
                    row, preserve_row_hash=preserve_row_hash, source_name=source.name
                )
            elif group is not None and group[3] > 1 and ordinal != group[1]:
                reason = "identical_duplicate_record"
                business_key = dict(zip(source.primary_key, key, strict=True))
                row_hash = _scd2_row_hash(
                    row, preserve_row_hash=preserve_row_hash, source_name=source.name
                )
                duplicate_row_count += 1
            else:
                continue

        excluded_ordinals.add(ordinal)
        if reason != "identical_duplicate_record":
            bad_row_count += 1
        quarantine_rows.append(
            {
                "row_ordinal": ordinal,
                "business_key": business_key,
                "raw_record": row,
                "row_hash": row_hash,
                "reason": reason,
            }
        )

    if invalid_key_rows:
        print(
            f"{source.name}: quarantined {invalid_key_rows:,} row(s) with null/blank "
            "business-key values."
        )

    return SCD2SnapshotAnalysis(
        conflicting_keys=frozenset(conflicting_keys),
        excluded_ordinals=frozenset(excluded_ordinals),
        quarantine_rows=tuple(quarantine_rows),
        conflict_details=tuple(conflict_details),
        bad_row_count=bad_row_count,
        business_key_count=conflict_key_count,
        duplicate_row_count=duplicate_row_count,
        invalid_key_row_count=invalid_key_rows,
    )


def _validate_scd2_snapshot(
    snapshot_path: Path,
    *,
    source: SourceConfig,
    key_columns: tuple[str, ...],
    preserve_row_hash: bool,
) -> tuple[list[tuple[tuple[Any, ...], Any, str, Any, str]], int]:
    """Compatibility wrapper returning conflict details and bad-row count."""
    analysis = _analyze_scd2_snapshot(
        snapshot_path,
        source=source,
        key_columns=key_columns,
        preserve_row_hash=preserve_row_hash,
    )
    return list(analysis.conflict_details), analysis.bad_row_count


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
            identifier = _identifier(column, "SCD2 key column")
            if isinstance(value, bool):
                literal = "TRUE" if value else "FALSE"
            elif isinstance(value, (int, float)):
                literal = str(value)
            else:
                literal = "'" + str(value).replace("'", "''") + "'"
            conditions.append(f'"{identifier}" = {literal}')
        predicates.append("(" + " AND ".join(conditions) + ")")

    return "NOT (" + " OR ".join(predicates) + ")"


def _destination_scope_matching_snapshot_keys(
    key_columns: tuple[str, ...],
    quarantined_keys: set[tuple[Any, ...]],
) -> str:
    """Limit SCD2 retirements to destination keys present in the clean snapshot.

    A snapshot containing a null/blank business key cannot safely identify which
    existing destination key that malformed row represents. Retiring any other
    absent destination row could therefore turn a malformed source row into a
    false deletion. Matching destination keys to the dlt staging table preserves
    all absent destination rows for this snapshot while still allowing updates
    to valid keys. Normal full-snapshot retirement resumes on the next clean run.
    """
    if not key_columns:
        raise ValueError("SCD2 destination scope requires business-key columns.")

    comparisons = []
    for column in key_columns:
        identifier = _identifier(column, "SCD2 key column")
        comparisons.append(f'source."{identifier}" = {{table}}."{identifier}"')
    scope = (
        "EXISTS (SELECT 1 FROM {staging_table} AS source WHERE "
        + " AND ".join(comparisons)
        + ")"
    )

    # Conflict groups are already excluded from the staged source. Keep the
    # explicit exclusion too, so this scope remains defensive if row filtering
    # changes in a future loader refactor.
    conflict_scope = _destination_scope_excluding_keys(key_columns, quarantined_keys)
    if conflict_scope:
        scope = f"({scope}) AND ({conflict_scope})"
    return scope


def _iter_scd2_snapshot_rows(
    snapshot_path: Path,
    *,
    key_columns: tuple[str, ...],
    date_column: str | None,
    since: str | None,
    preserve_row_hash: bool,
    source_name: str,
    quarantined_keys: set[tuple[Any, ...]],
    excluded_ordinals: frozenset[int] = frozenset(),
) -> Iterator[dict[str, Any]]:
    """Yield one clean row per key, excluding every quarantined source ordinal."""
    seen_keys: set[tuple[Any, ...]] = set()

    for row, ordinal in _iter_snapshot_rows_with_ordinals(
        snapshot_path, date_column, since
    ):
        if ordinal in excluded_ordinals:
            continue
        key = tuple(row.get(column) for column in key_columns)
        if key in quarantined_keys or key in seen_keys:
            continue
        seen_keys.add(key)
        row_hash = _scd2_row_hash(
            row, preserve_row_hash=preserve_row_hash, source_name=source_name
        )
        if preserve_row_hash:
            hash_key = next(key for key in row if key.lower() == "row_hash")
            if hash_key != "row_hash":
                row.pop(hash_key)
        row["row_hash"] = row_hash
        yield row


def _quarantine_context(
    snapshot_path: Path, source_name: str, context: dict[str, Any] | None
) -> dict[str, Any]:
    """Return validated archive provenance, with stable local identity as fallback."""
    source_sha = _snapshot_sha256(snapshot_path)
    value = dict(context or {})
    detected_at = datetime.now(UTC).isoformat()
    return {
        "snapshot_id": str(value.get("snapshot_id") or f"local:{source_sha}"),
        "source_name": source_name,
        "source_run_id": str(value.get("source_run_id") or "local"),
        "source_run_attempt": int(value.get("source_run_attempt") or 1),
        "extracted_at_utc": value.get("extracted_at_utc") or detected_at,
        "source_commit": str(value.get("source_commit") or "local"),
        "release_tag": value.get("release_tag") or "local",
        "asset_name": value.get("asset_name") or snapshot_path.name,
        "parquet_sha256": str(value.get("parquet_sha256") or source_sha),
        "detected_at_utc": detected_at,
    }


def _quarantine_values(
    analysis: SCD2SnapshotAnalysis, context: dict[str, Any]
) -> list[tuple[Any, ...]]:
    values: list[tuple[Any, ...]] = []
    for item in analysis.quarantine_rows:
        ordinal = int(item["row_ordinal"])
        reason = str(item["reason"])
        quarantine_id = hashlib.sha256(
            f"{context['snapshot_id']}|{context['source_name']}|{ordinal}|{reason}".encode()
        ).hexdigest()
        values.append(
            (
                quarantine_id,
                context["snapshot_id"],
                context["source_name"],
                context["source_run_id"],
                context["source_run_attempt"],
                context["extracted_at_utc"],
                context["source_commit"],
                context["release_tag"],
                context["asset_name"],
                context["parquet_sha256"],
                ordinal,
                json.dumps(item["business_key"], sort_keys=True, default=str),
                json.dumps(item["raw_record"], sort_keys=True, default=str),
                str(item["row_hash"]),
                reason,
                context["detected_at_utc"],
            )
        )
    return values


def _persist_duckdb_quarantine(
    database: str, dataset: str, values: list[tuple[Any, ...]]
) -> None:
    import duckdb

    schema = _identifier(dataset, "dataset name").lower()
    with duckdb.connect(database) as connection:
        connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        connection.execute(
            f'''CREATE TABLE IF NOT EXISTS "{schema}".toronto_scd2_quarantine (
                quarantine_id VARCHAR PRIMARY KEY,
                snapshot_id VARCHAR NOT NULL,
                source_name VARCHAR NOT NULL,
                source_run_id VARCHAR NOT NULL,
                source_run_attempt INTEGER NOT NULL,
                extracted_at_utc TIMESTAMPTZ,
                source_commit VARCHAR,
                release_tag VARCHAR,
                asset_name VARCHAR,
                parquet_sha256 VARCHAR,
                row_ordinal BIGINT NOT NULL,
                business_key_json VARCHAR,
                raw_record_json VARCHAR NOT NULL,
                row_hash VARCHAR NOT NULL,
                reason VARCHAR NOT NULL,
                detected_at_utc TIMESTAMPTZ NOT NULL,
                UNIQUE(snapshot_id, source_name, row_ordinal, reason)
            )'''
        )
        if values:
            connection.executemany(
                f'''INSERT INTO "{schema}".toronto_scd2_quarantine VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (quarantine_id) DO UPDATE SET
                        raw_record_json = excluded.raw_record_json,
                        row_hash = excluded.row_hash,
                        detected_at_utc = excluded.detected_at_utc''',
                values,
            )


def _persist_snowflake_quarantine(
    credentials: str, dataset: str, values: list[tuple[Any, ...]]
) -> None:
    """Bulk-stage quarantine rows, then MERGE them idempotently into Snowflake."""
    if not values:
        return

    from snowflake import connector

    parts = urlsplit(credentials)
    params = parse_qs(parts.query)
    if parts.scheme != "snowflake" or not parts.hostname or not parts.username:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is not a valid Snowflake URI.")
    encoded = params.get("private_key", [""])[0].replace(" ", "+")
    key_bytes = base64.b64decode(encoded, validate=True)
    if not key_bytes:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION contains an empty private key.")
    schema = _identifier(dataset, "Snowflake dataset name").upper()
    target = f'TORONTO."{schema}".TORONTO_SCD2_QUARANTINE'
    upload_table = "TORONTO_SCD2_QUARANTINE_UPLOAD"
    columns = (
        "QUARANTINE_ID, SNAPSHOT_ID, SOURCE_NAME, SOURCE_RUN_ID, "
        "SOURCE_RUN_ATTEMPT, EXTRACTED_AT_UTC, SOURCE_COMMIT, RELEASE_TAG, "
        "ASSET_NAME, PARQUET_SHA256, ROW_ORDINAL, BUSINESS_KEY_JSON, "
        "RAW_RECORD_JSON, ROW_HASH, REASON, DETECTED_AT_UTC"
    )
    placeholders = ", ".join(["%s"] * 16)
    with tempfile.TemporaryDirectory(prefix="toronto-quarantine-key-") as temp:
        key_path = Path(temp) / "snowflake_key.p8"
        key_path.write_bytes(key_bytes)
        key_path.chmod(0o600)
        connection = connector.connect(
            account=parts.hostname,
            user=unquote(parts.username),
            private_key_file=str(key_path),
            role="TORONTO_INGEST_ROLE",
            warehouse="TORONTO_XS_WH",
            database="TORONTO",
        )
        cursor = connection.cursor()
        try:
            # Quarantine is persisted before the first dlt load creates its
            # run-scoped destination schema, so initialize that schema here.
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS TORONTO."{schema}"')
            cursor.execute(f'USE SCHEMA TORONTO."{schema}"')
            cursor.execute(
                f"""CREATE TABLE IF NOT EXISTS {target} (
                    QUARANTINE_ID VARCHAR NOT NULL,
                    SNAPSHOT_ID VARCHAR NOT NULL,
                    SOURCE_NAME VARCHAR NOT NULL,
                    SOURCE_RUN_ID VARCHAR NOT NULL,
                    SOURCE_RUN_ATTEMPT NUMBER(10,0) NOT NULL,
                    EXTRACTED_AT_UTC TIMESTAMP_TZ,
                    SOURCE_COMMIT VARCHAR,
                    RELEASE_TAG VARCHAR,
                    ASSET_NAME VARCHAR,
                    PARQUET_SHA256 VARCHAR,
                    ROW_ORDINAL NUMBER(38,0) NOT NULL,
                    BUSINESS_KEY_JSON VARCHAR,
                    RAW_RECORD_JSON VARCHAR NOT NULL,
                    ROW_HASH VARCHAR NOT NULL,
                    REASON VARCHAR NOT NULL,
                    DETECTED_AT_UTC TIMESTAMP_TZ NOT NULL
                )"""
            )
            cursor.execute(
                f'''CREATE TEMPORARY TABLE "{upload_table}" AS
                    SELECT {columns} FROM {target} WHERE 1 = 0'''
            )
            cursor.executemany(
                f'INSERT INTO "{upload_table}" ({columns}) VALUES ({placeholders})',
                values,
            )
            cursor.execute(
                f'''MERGE INTO {target} AS target
                    USING (SELECT {columns} FROM "{upload_table}") AS incoming
                    ON target.QUARANTINE_ID = incoming.QUARANTINE_ID
                    WHEN MATCHED THEN UPDATE SET
                        RAW_RECORD_JSON = incoming.RAW_RECORD_JSON,
                        ROW_HASH = incoming.ROW_HASH,
                        DETECTED_AT_UTC = incoming.DETECTED_AT_UTC
                    WHEN NOT MATCHED THEN INSERT ({columns})
                    VALUES (
                        incoming.QUARANTINE_ID, incoming.SNAPSHOT_ID,
                        incoming.SOURCE_NAME, incoming.SOURCE_RUN_ID,
                        incoming.SOURCE_RUN_ATTEMPT, incoming.EXTRACTED_AT_UTC,
                        incoming.SOURCE_COMMIT, incoming.RELEASE_TAG, incoming.ASSET_NAME,
                        incoming.PARQUET_SHA256, incoming.ROW_ORDINAL,
                        incoming.BUSINESS_KEY_JSON, incoming.RAW_RECORD_JSON,
                        incoming.ROW_HASH, incoming.REASON, incoming.DETECTED_AT_UTC
                    )'''
            )
            cursor.execute(f'DROP TABLE "{upload_table}"')
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()


def _persist_quarantine(
    *,
    destination_kind: str,
    credentials: str,
    dataset: str,
    context: dict[str, Any],
    analysis: SCD2SnapshotAnalysis,
    audit_database: str | None = None,
) -> None:
    values = _quarantine_values(analysis, context)
    if destination_kind == "duckdb":
        _persist_duckdb_quarantine(credentials, dataset, values)
        if audit_database:
            _persist_duckdb_quarantine(audit_database, dataset, values)
    elif destination_kind == "snowflake":
        _persist_snowflake_quarantine(credentials, dataset, values)


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
    snapshot_context: dict[str, Any] | None = None,
    quarantine_audit_credentials: str | None = None,
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
    quarantined_bad_row_count = 0
    quarantined_business_key_count = 0
    quarantine_digest: str | None = None
    analysis: SCD2SnapshotAnalysis | None = None
    destination_scope: str | None = None

    if destination_kind == "duckdb":
        configured_path = Path(credentials)
        if not configured_path.is_absolute():
            configured_path = Path(__file__).resolve().parents[2] / configured_path
        credentials = str(configured_path.resolve())

    if strategy == "scd2":
        key_columns = _resolve_scd2_key_columns(path, source)
        analysis = _analyze_scd2_snapshot(
            path,
            source=source,
            key_columns=key_columns,
            preserve_row_hash=preserve_row_hash,
        )
        quarantined_key_values = set(analysis.conflicting_keys)
        quarantined_row_count = len(analysis.quarantine_rows)
        quarantined_bad_row_count = analysis.bad_row_count
        quarantined_business_key_count = analysis.business_key_count
        quarantined_details = tuple(
            {
                "business_key": dict(zip(source.primary_key, key, strict=True)),
                "source_ids": [first_id, second_id],
                "row_hash_prefixes": [first_hash[:12], second_hash[:12]],
            }
            for key, first_id, first_hash, second_id, second_hash in analysis.conflict_details
        )
        context = _quarantine_context(path, source.name, snapshot_context)
        quarantine_values = _quarantine_values(analysis, context)
        quarantine_digest = hashlib.sha256(
            "".join(f"{row[0]}\n" for row in quarantine_values).encode("ascii")
        ).hexdigest()
        _persist_quarantine(
            destination_kind=destination_kind,
            credentials=credentials,
            dataset=dataset,
            context=context,
            analysis=analysis,
            audit_database=quarantine_audit_credentials,
        )
        if analysis.invalid_key_row_count:
            destination_scope = _destination_scope_matching_snapshot_keys(
                key_columns, quarantined_key_values
            )
        else:
            # dlt persists destination_scope in the pipeline schema. Omitting the
            # hint on a later clean snapshot does not clear a scope left by an
            # earlier incomplete snapshot, so explicitly restore full-table
            # retirement when there are no conflicting keys to protect.
            destination_scope = (
                _destination_scope_excluding_keys(key_columns, quarantined_key_values)
                or "1 = 1"
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
                excluded_ordinals=(
                    analysis.excluded_ordinals if analysis else frozenset()
                ),
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
        quarantined_bad_row_count=quarantined_bad_row_count,
        quarantined_business_key_count=quarantined_business_key_count,
        quarantine_sha256=quarantine_digest,
    )
