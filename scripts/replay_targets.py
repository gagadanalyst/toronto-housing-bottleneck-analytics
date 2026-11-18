"""Candidate warehouse adapters and orchestration for immutable snapshot replay.

The archive reader and SQLite ledger own their respective concerns. This module
connects them to the existing SCD2 loader and refuses production write targets.
"""

from __future__ import annotations

import base64
import csv
import gzip
import os
import re
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from scripts import replay_warehouse_ledger
from scripts.replay_archive import SOURCE_NAMES, SnapshotArchive, extract_source
from scripts.replay_ledger import ReplayLedger

DEFAULT_DUCKDB_ROOT = Path("data/recovery/snapshot-replay/duckdb")
INCREMENTAL_DUCKDB_ROOT = Path("data/staging/duckdb")
DUCKDB_INPUT_STAGE_SCHEMA = "snapshot_input_stage"
SNOWFLAKE_INPUT_STAGE_SCHEMA_RE = re.compile(r"RAW_INPUT_STAGE_[0-9]+_[0-9]+\Z")
REBUILD_SNOWFLAKE_SCHEMA_RE = re.compile(r"TORONTO_SNAPSHOT_REPLAY_[0-9]+\Z")
SNOWFLAKE_SCHEMA_RE = (
    REBUILD_SNOWFLAKE_SCHEMA_RE  # Backward-compatible test/import alias
)
INCREMENTAL_SNOWFLAKE_SCHEMA_RE = re.compile(r"RAW_STAGE_([0-9]+)_([0-9]+)\Z")
PRODUCTION_PIPELINES = {
    "active_permits": "toronto_active_permits_scd2",
    "cleared_permits": "toronto_cleared_permits_scd2",
}
EXPECTED_SNOWFLAKE_ACCOUNT = "bihaste-tu44125"
IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
EXPORT_COLUMNS = (
    "permit_num",
    "revision_num",
    "permit_type",
    "row_hash",
    "valid_from",
    "valid_to",
)


def _identifier(value: str, label: str) -> str:
    if not IDENTIFIER_RE.fullmatch(str(value)):
        raise ValueError(f"Unsafe {label}: {value!r}.")
    return str(value)


def _utc(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{label} must be an ISO-8601 timestamp.") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must include a timezone.")
    return parsed.astimezone(UTC)


def source_boundary(archive: SnapshotArchive, source_name: str) -> datetime:
    """Use the source extraction-start timestamp as its SCD2 observation boundary."""
    source = archive.sources[source_name]
    return _utc(source.extracted_at, f"{source_name} extraction timestamp")


def _input_stage_config(source_config: Any) -> Any:
    """Make a full-snapshot replace config for the raw input staging table."""
    return replace(
        source_config,
        warehouse_strategy="replace",
        warehouse_since=None,
        warehouse_date_column=None,
    )


def _validate_staged_snapshot(
    *,
    source_name: str,
    expected_rows: int,
    expected_columns: tuple[str, ...],
    actual_rows: int,
    actual_columns: list[str] | tuple[str, ...],
) -> None:
    """Require the materialized input stage to match its immutable archive contract."""
    if int(actual_rows) != int(expected_rows):
        raise ValueError(
            f"{source_name}: input staging row count differs from archive: "
            f"expected={expected_rows}, actual={actual_rows}."
        )
    actual = {str(column).lower() for column in actual_columns}
    missing = sorted({str(column).lower() for column in expected_columns} - actual)
    if missing:
        raise ValueError(
            f"{source_name}: input staging table is missing archived columns: {missing}."
        )


def _warehouse_window_filter(source_config: Any, *, snowflake: bool) -> tuple[str, Any]:
    """Build the same inclusive date-window predicate used by the SCD2 loader."""
    date_column = getattr(source_config, "warehouse_date_column", None)
    since = getattr(source_config, "warehouse_since", None)
    if date_column is None and since is None:
        return "", None
    if not date_column or not since:
        raise ValueError(
            "Warehouse date filtering requires both warehouse_date_column "
            "and warehouse_since."
        )
    column = _identifier(str(date_column), "warehouse date column")
    if snowflake:
        expression = f'SUBSTR(TO_VARCHAR("{column.upper()}"), 1, 10) >= %s'
    else:
        expression = f'SUBSTR(CAST("{column}" AS VARCHAR), 1, 10) >= ?'
    return f" WHERE {expression}", str(since)


def _analyse_mode1_snapshot(
    parquet_path: Path,
    source_name: str,
    source_config: Any,
    *,
    warehouse_rows: int,
    baseline_current_rows: int,
) -> dict[str, Any]:
    """Run the canonical SCD2 row/key analysis before candidate SCD2 writes.

    The functions are shared with ``load_scd2`` so preflight and ingestion agree
    about invalid keys, conflicting duplicate groups, and identical duplicates.
    Bad rows remain subject to the existing quarantine-generation promotion gate;
    they are not silently discarded or allowed into the clean SCD2 table.
    """
    from toronto_housing_ingestion.dlt_loader import (
        _analyze_scd2_snapshot,
        _resolve_scd2_key_columns,
    )

    key_columns = _resolve_scd2_key_columns(parquet_path, source_config)
    analysis = _analyze_scd2_snapshot(
        parquet_path,
        source=source_config,
        key_columns=key_columns,
        preserve_row_hash=False,
    )

    conflicting_rows = sum(
        item["reason"] == "conflicting_duplicate_business_key"
        for item in analysis.quarantine_rows
    )
    total_quarantined = len(analysis.quarantine_rows)
    accounted = (
        analysis.invalid_key_row_count + conflicting_rows + analysis.duplicate_row_count
    )
    if accounted != total_quarantined:
        raise ValueError(
            f"{source_name}: staged-source key analysis did not account for every "
            "quarantined row."
        )
    if int(analysis.bad_row_count) != (
        int(analysis.invalid_key_row_count) + int(conflicting_rows)
    ):
        raise ValueError(
            f"{source_name}: staged-source bad-row analysis is inconsistent."
        )

    maximum_change = getattr(source_config, "max_row_change_percent", None)
    row_change_percent: float | None = None
    if warehouse_rows < 1:
        raise ValueError(
            f"{source_name}: staged snapshot contains no rows inside the configured "
            "warehouse date window."
        )
    if maximum_change is not None:
        maximum_change = float(maximum_change)
        if not 0 <= maximum_change <= 100:
            raise ValueError(
                f"{source_name}: max_row_change_percent must be between 0 and 100."
            )
        if baseline_current_rows < 1:
            raise ValueError(
                f"{source_name}: cannot evaluate the configured row-change gate "
                "against an empty production-derived candidate."
            )
        row_change_percent = (
            abs(warehouse_rows - baseline_current_rows) / baseline_current_rows * 100.0
        )
        if row_change_percent > maximum_change:
            raise ValueError(
                f"{source_name}: staged warehouse row count changed by "
                f"{row_change_percent:.2f}% ({baseline_current_rows} -> "
                f"{warehouse_rows}), exceeding the configured "
                f"{maximum_change:.2f}% limit. SCD2 was not applied."
            )

    return {
        "status": "passed",
        "warehouse_rows": int(warehouse_rows),
        "baseline_current_rows": int(baseline_current_rows),
        "row_change_percent": (
            round(row_change_percent, 4) if row_change_percent is not None else None
        ),
        "max_row_change_percent": maximum_change,
        "invalid_business_key_rows": int(analysis.invalid_key_row_count),
        "conflicting_duplicate_key_groups": int(analysis.business_key_count),
        "conflicting_duplicate_rows": int(conflicting_rows),
        "identical_duplicate_rows": int(analysis.duplicate_row_count),
        "quarantined_bad_rows": int(analysis.bad_row_count),
        "quarantined_total_rows": total_quarantined,
        "key_handling": "quarantine_before_scd2",
    }


def _verify_scd2_quarantine_matches_preflight(
    source_name: str, preflight: dict[str, Any] | None, result: Any
) -> None:
    """Fail the candidate if the SCD2 loader disagrees with Mode 1 preflight."""
    if not preflight:
        return
    expected = (
        int(preflight["quarantined_total_rows"]),
        int(preflight["quarantined_bad_rows"]),
        int(preflight["conflicting_duplicate_key_groups"]),
    )
    actual = (
        int(getattr(result, "quarantined_row_count", 0) or 0),
        int(getattr(result, "quarantined_bad_row_count", 0) or 0),
        int(getattr(result, "quarantined_business_key_count", 0) or 0),
    )
    if actual != expected:
        raise ValueError(
            f"{source_name}: SCD2 quarantine counts differ from the staged-source "
            f"preflight: expected={expected}, actual={actual}."
        )


def validate_source_contract(
    archive: SnapshotArchive, source_name: str, source_config: Any
) -> None:
    """Check archived columns, keys and minimum size against ingestion config."""
    source = archive.sources[source_name]
    if source.error:
        raise ValueError(source.error)

    columns = {column.lower() for column in source.columns}
    required = {column.lower() for column in source_config.required_columns}
    keys = {column.lower() for column in source_config.primary_key}
    missing = sorted((required | keys) - columns)
    if missing:
        raise ValueError(
            f"{source_name}: archive is missing configured columns: {missing}."
        )

    minimum = source_config.min_row_count
    if minimum is not None and source.row_count < minimum:
        raise ValueError(
            f"{source_name}: archive has {source.row_count} rows; "
            f"configured minimum is {minimum}."
        )

    declared_key = source.manifest.get("business_key") or source.manifest.get(
        "primary_key"
    )
    if declared_key:
        if (
            not isinstance(declared_key, list)
            or {str(value).lower() for value in declared_key} != keys
        ):
            raise ValueError(
                f"{source_name}: archive business key disagrees with "
                "warehouse configuration."
            )


def candidate_database_path(
    database: Path,
    *,
    mode: str = "rebuild",
    candidate_root: Path | None = None,
    production_database: Path | None = None,
) -> Path:
    """Resolve a mode-specific candidate path, never the production database."""
    if mode not in {"rebuild", "incremental"}:
        raise ValueError(f"Unsupported replay mode: {mode!r}.")
    expected_root = candidate_root or (
        INCREMENTAL_DUCKDB_ROOT if mode == "incremental" else DEFAULT_DUCKDB_ROOT
    )
    root = Path(expected_root).expanduser().resolve()
    candidate = Path(database).expanduser().resolve()
    if candidate == root or not candidate.is_relative_to(root):
        raise ValueError(f"DuckDB {mode} candidate must be inside {root}.")
    if candidate.suffix.lower() != ".duckdb":
        raise ValueError("DuckDB candidate must use a .duckdb file.")
    if production_database is not None:
        production = Path(production_database).expanduser().resolve()
        if candidate == production:
            raise ValueError(
                "Refusing to use the configured production DuckDB database."
            )
    return candidate


def _normalise_export_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _export_cursor(cursor: Any, output: Path) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    with gzip.open(output, "wt", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(EXPORT_COLUMNS)
        while rows := cursor.fetchmany(10_000):
            writer.writerows(
                [_normalise_export_value(value) for value in row] for row in rows
            )
            rows_written += len(rows)
    return rows_written


def validation_queries(
    table: str, primary_key: tuple[str, ...], *, snowflake: bool
) -> dict[str, str]:
    """Return equivalent SCD2 integrity checks for each SQL dialect."""

    def quote(name: str) -> str:
        identifier = _identifier(name, "column")
        return f'"{identifier.upper() if snowflake else identifier}"'

    keys = ", ".join(quote(name) for name in primary_key)
    nulls = " OR ".join(
        f"({quote(name)} IS NULL OR TRIM(CAST({quote(name)} AS VARCHAR)) = '')"
        for name in primary_key
    )
    if snowflake:
        future = "'9999-12-31 00:00:00 +00:00'::TIMESTAMP_TZ"
        return {
            "counts": (
                f"SELECT COUNT(*), COUNT_IF(_DLT_VALID_TO IS NULL), "
                f"COUNT_IF(_DLT_VALID_TO IS NOT NULL) FROM {table}"
            ),
            "invalid": (
                "SELECT COUNT_IF(_DLT_VALID_FROM IS NULL OR "
                "(_DLT_VALID_TO IS NOT NULL AND "
                f"_DLT_VALID_TO <= _DLT_VALID_FROM)) FROM {table}"
            ),
            "overlap": f"""WITH ordered AS (
                SELECT _DLT_VALID_FROM,
                    MAX(COALESCE(_DLT_VALID_TO, {future})) OVER (
                        PARTITION BY {keys} ORDER BY _DLT_VALID_FROM
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    ) AS PREVIOUS_MAX_TO
                FROM {table}
            ) SELECT COUNT_IF(PREVIOUS_MAX_TO > _DLT_VALID_FROM) FROM ordered""",
            "duplicate": (
                f"SELECT COUNT(*) FROM (SELECT {keys} FROM {table} "
                f"WHERE _DLT_VALID_TO IS NULL GROUP BY {keys} HAVING COUNT(*) > 1)"
            ),
            "null_keys": f"SELECT COUNT_IF({nulls}) FROM {table}",
        }
    future = "TIMESTAMPTZ '9999-12-31 00:00:00+00'"
    return {
        "counts": (
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE _dlt_valid_to IS NULL), "
            "COUNT(*) FILTER (WHERE _dlt_valid_to IS NOT NULL) "
            f"FROM {table}"
        ),
        "invalid": (
            f"SELECT COUNT(*) FROM {table} WHERE _dlt_valid_from IS NULL "
            "OR (_dlt_valid_to IS NOT NULL AND "
            "_dlt_valid_to <= _dlt_valid_from)"
        ),
        "overlap": f"""WITH ordered AS (
            SELECT _dlt_valid_from,
                MAX(COALESCE(_dlt_valid_to, {future})) OVER (
                    PARTITION BY {keys} ORDER BY _dlt_valid_from
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) AS previous_max_to
            FROM {table}
        ) SELECT COUNT(*) FROM ordered WHERE previous_max_to > _dlt_valid_from""",
        "duplicate": (
            f"SELECT COUNT(*) FROM (SELECT {keys} FROM {table} "
            f"WHERE _dlt_valid_to IS NULL GROUP BY {keys} HAVING COUNT(*) > 1)"
        ),
        "null_keys": f"SELECT COUNT(*) FROM {table} WHERE {nulls}",
    }


def _validate_metrics(metrics: dict[str, int]) -> None:
    failures = {
        name: metrics[name]
        for name in (
            "invalid_intervals",
            "overlapping_intervals",
            "duplicate_current_business_keys",
            "null_business_keys",
        )
        if metrics[name]
    }
    if (
        not metrics["total"]
        or metrics["total"] != metrics["current"] + metrics["expired"]
        or failures
    ):
        raise ValueError(f"SCD2 integrity checks failed: {metrics}.")


def _source_export_sql(source: Any, *, snowflake: bool) -> str:
    names = [*source.primary_key, "row_hash", "_dlt_valid_from", "_dlt_valid_to"]
    quoted = []
    for name in names:
        identifier = _identifier(name, "export column")
        sql_name = identifier.upper() if snowflake else identifier
        quoted.append(f'"{sql_name}"')
    aliases = [*source.primary_key, "row_hash", "valid_from", "valid_to"]
    return "SELECT " + ", ".join(
        f'{column} AS "{alias}"' for column, alias in zip(quoted, aliases, strict=True)
    )


class DuckDBReplayTarget:
    """One isolated DuckDB candidate; the production database is never a target."""

    backend = "duckdb"

    def __init__(
        self,
        database: Path,
        *,
        dataset_name: str = "raw",
        production_database: Path = Path("data/warehouse/toronto.duckdb"),
        mode: str = "rebuild",
    ) -> None:
        # Fail immediately if the optional runtime dependency is absent.
        import duckdb  # noqa: F401

        self.mode = mode
        self.database = candidate_database_path(
            database, mode=mode, production_database=production_database
        )
        self.input_stage_database = self.database.with_name(
            f"{self.database.stem}.input-stage.duckdb"
        )
        if self.input_stage_database == self.database:
            raise ValueError(
                "DuckDB input stage and SCD2 candidate must be separate files."
            )
        if mode == "incremental" and not self.database.is_file():
            raise FileNotFoundError(
                "Incremental DuckDB loading requires a prepared production candidate; "
                "run scripts/prepare_duckdb_candidate.py first."
            )
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.dataset = _identifier(dataset_name, "DuckDB dataset")
        from toronto_housing_ingestion.dlt_loader import ensure_duckdb_database

        ensure_duckdb_database(str(self.database), self.dataset)
        replay_warehouse_ledger.ensure_ledger(self)

    def completed_snapshot_ids(self) -> set[str]:
        return replay_warehouse_ledger.completed_snapshot_ids(self)

    def snapshot_status(self, snapshot_id: str, source_name: str) -> str | None:
        return replay_warehouse_ledger.snapshot_status(self, snapshot_id, source_name)

    def register_archive(self, archive: SnapshotArchive) -> None:
        replay_warehouse_ledger.register_archive(self, archive)

    def assert_can_load(self, archive: SnapshotArchive, source_name: str) -> None:
        replay_warehouse_ledger.assert_replay_order(self, archive, source_name)

    def set_snapshot_status(
        self, archive: SnapshotArchive, source_name: str, status: str, **kwargs: Any
    ) -> None:
        replay_warehouse_ledger.set_snapshot_status(
            self, archive, source_name, status, **kwargs
        )

    def _current_scd2_rows(self, source_config: Any) -> int:
        """Count current rows in the production-derived candidate before this load."""
        import duckdb

        table = _identifier(source_config.warehouse_table, "warehouse table")
        qualified = f'"{self.dataset}"."{table}"'
        with duckdb.connect(str(self.database), read_only=True) as connection:
            return int(
                connection.execute(
                    f'SELECT COUNT(*) FROM {qualified} WHERE "_dlt_valid_to" IS NULL'
                ).fetchone()[0]
                or 0
            )

    def _stage_input(
        self,
        archive: SnapshotArchive,
        source_name: str,
        parquet_path: Path,
        source_config: Any,
    ) -> dict[str, Any] | None:
        """Materialize and verify the incoming snapshot before candidate SCD2 writes."""
        if self.mode != "incremental":
            return
        import duckdb

        from toronto_housing_ingestion.dlt_loader import load_scd2

        stage_config = _input_stage_config(source_config)
        load_scd2(
            credentials=str(self.input_stage_database),
            snapshot_path=parquet_path,
            source=stage_config,
            dataset_name=DUCKDB_INPUT_STAGE_SCHEMA,
            destination_kind="duckdb",
            pipeline_name=f"toronto_input_stage_{source_name}",
        )
        table = _identifier(source_config.warehouse_table, "staging table")
        with duckdb.connect(
            str(self.input_stage_database), read_only=True
        ) as connection:
            qualified = f'"{DUCKDB_INPUT_STAGE_SCHEMA}"."{table}"'
            actual_rows = int(
                connection.execute(f"SELECT COUNT(*) FROM {qualified}").fetchone()[0]
            )
            cursor = connection.execute(f"SELECT * FROM {qualified} LIMIT 0")
            actual_columns = [column[0] for column in (cursor.description or [])]
            where, parameter = _warehouse_window_filter(source_config, snowflake=False)
            count_query = f"SELECT COUNT(*) FROM {qualified}{where}"
            warehouse_rows = int(
                connection.execute(
                    count_query, [parameter] if parameter is not None else []
                ).fetchone()[0]
                or 0
            )
        source = archive.sources[source_name]
        _validate_staged_snapshot(
            source_name=source_name,
            expected_rows=source.row_count,
            expected_columns=source.columns,
            actual_rows=actual_rows,
            actual_columns=actual_columns,
        )
        quality = _analyse_mode1_snapshot(
            parquet_path,
            source_name,
            source_config,
            warehouse_rows=warehouse_rows,
            baseline_current_rows=self._current_scd2_rows(source_config),
        )
        return {
            "schema": DUCKDB_INPUT_STAGE_SCHEMA,
            "table": table,
            "rows": actual_rows,
            "quality": quality,
        }

    def load(
        self,
        archive: SnapshotArchive,
        source_name: str,
        parquet_path: Path,
        source_config: Any,
    ) -> dict[str, Any]:
        from toronto_housing_ingestion.dlt_loader import load_scd2

        input_stage = self._stage_input(
            archive, source_name, parquet_path, source_config
        )
        source = archive.sources[source_name]
        boundary = source_boundary(archive, source_name)
        result = load_scd2(
            credentials=str(self.database),
            snapshot_path=parquet_path,
            source=source_config,
            dataset_name=self.dataset,
            destination_kind="duckdb",
            # Keep the canonical pipeline identity in both modes. The database/schema
            # is already isolated for Mode 2, so separate pipeline names are not
            # needed and would make a promoted rebuild incompatible with the next
            # routine production load.
            pipeline_name=PRODUCTION_PIPELINES[source_name],
            boundary_timestamp=boundary.isoformat(),
            quarantine_audit_credentials=(
                os.environ.get("TORONTO_QUARANTINE_AUDIT_DUCKDB_PATH")
                if self.mode == "incremental"
                else None
            ),
            snapshot_context={
                "snapshot_id": archive.snapshot_id,
                "source_run_id": archive.run_id,
                "source_run_attempt": archive.run_attempt,
                "extracted_at_utc": boundary.isoformat(),
                "source_commit": archive.source_commit,
                "release_tag": archive.release_tag,
                "asset_name": archive.asset_name,
                "parquet_sha256": source.parquet_sha256,
                "archive_sha256": archive.archive_sha256,
            },
        )
        _verify_scd2_quarantine_matches_preflight(
            source_name,
            (input_stage or {}).get("quality"),
            result,
        )
        return {
            "table": result.table_name,
            "total_rows": result.total_rows,
            "current_rows": result.current_rows,
            "quarantined_rows": result.quarantined_row_count,
            "quarantined_bad_rows": result.quarantined_bad_row_count,
            "quarantined_business_keys": result.quarantined_business_key_count,
            "quarantine_sha256": result.quarantine_sha256,
            "input_stage": input_stage,
        }

    def finalize(self) -> None:
        import duckdb

        with duckdb.connect(str(self.database)) as connection:
            connection.execute("CHECKPOINT")
        if self.mode == "incremental" and self.input_stage_database.is_file():
            with duckdb.connect(str(self.input_stage_database)) as connection:
                connection.execute("CHECKPOINT")
            self.input_stage_database.unlink(missing_ok=True)
            Path(f"{self.input_stage_database}.wal").unlink(missing_ok=True)

    def validate_and_export(
        self, source_configs: dict[str, Any], output_dir: Path
    ) -> dict[str, Any]:
        import duckdb

        results: dict[str, Any] = {}
        with duckdb.connect(str(self.database), read_only=True) as connection:
            connection.execute("SET TimeZone='UTC'")
            for source_name, source in source_configs.items():
                try:
                    table_name = _identifier(source.warehouse_table, "warehouse table")
                    table = f'"{self.dataset}"."{table_name}"'
                    checks = validation_queries(
                        table, source.primary_key, snowflake=False
                    )
                    total, current, expired = connection.execute(
                        checks["counts"]
                    ).fetchone()
                    metrics = {
                        "total": int(total or 0),
                        "current": int(current or 0),
                        "expired": int(expired or 0),
                        "invalid_intervals": int(
                            connection.execute(checks["invalid"]).fetchone()[0] or 0
                        ),
                        "overlapping_intervals": int(
                            connection.execute(checks["overlap"]).fetchone()[0] or 0
                        ),
                        "duplicate_current_business_keys": int(
                            connection.execute(checks["duplicate"]).fetchone()[0] or 0
                        ),
                        "null_business_keys": int(
                            connection.execute(checks["null_keys"]).fetchone()[0] or 0
                        ),
                    }
                    _validate_metrics(metrics)
                    table_sql = _source_export_sql(source, snowflake=False)
                    cursor = connection.execute(f"{table_sql} FROM {table}")
                    output = output_dir / f"{table_name}_history.csv.gz"
                    exported = _export_cursor(cursor, output)
                    if exported != metrics["total"]:
                        raise ValueError(
                            "History export row count does not match table count."
                        )
                    results[source_name] = {
                        "status": "passed",
                        **metrics,
                        "export": str(output),
                        "export_rows": exported,
                    }
                except Exception as exc:
                    results[source_name] = {
                        "status": "failed",
                        "error": f"{type(exc).__name__}: {exc}"[:4000],
                    }
        status = (
            "passed"
            if all(value["status"] == "passed" for value in results.values())
            else "failed"
        )
        return {"status": status, "sources": results}


def snowflake_input_stage_schema(candidate_schema: str) -> str | None:
    """Derive the per-run input stage only from an approved incremental candidate."""
    match = INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch(str(candidate_schema).upper())
    if match is None:
        return None
    return f"RAW_INPUT_STAGE_{match.group(1)}_{match.group(2)}"


def _validate_snowflake_account_context(
    role: Any, database: Any, warehouse: Any, organization: Any, account: Any
) -> None:
    """Fail before DDL unless the connection is the expected project session."""
    if (
        str(role).upper() != "TORONTO_INGEST_ROLE"
        or str(database).upper() != "TORONTO"
        or str(warehouse).upper() != "TORONTO_XS_WH"
        or f"{organization}-{account}".lower() != EXPECTED_SNOWFLAKE_ACCOUNT
    ):
        raise ValueError(
            "Unexpected Snowflake replay session: "
            f"role={role}, database={database}, warehouse={warehouse}."
        )


def _validate_snowflake_schema_owner(schema: str, owner: Any) -> None:
    """Require the ingest role to own every replay/candidate schema."""
    if owner is None or str(owner[0]).upper() != "TORONTO_INGEST_ROLE":
        raise ValueError(
            f"Snowflake replay schema {schema} must be owned by "
            f"TORONTO_INGEST_ROLE; observed={owner!r}."
        )


class SnowflakeReplayTarget:
    """A run-scoped Snowflake candidate schema, never TORONTO.RAW."""

    backend = "snowflake"

    def __init__(self, credentials: str, schema: str, *, mode: str = "rebuild") -> None:
        from scripts.snowflake_operations import require_snowflake_operations_enabled

        require_snowflake_operations_enabled("Snowflake snapshot replay")
        schema = str(schema).upper()
        if mode not in {"rebuild", "incremental"}:
            raise ValueError(f"Unsupported replay mode: {mode!r}.")
        allowed_schema = (
            INCREMENTAL_SNOWFLAKE_SCHEMA_RE
            if mode == "incremental"
            else REBUILD_SNOWFLAKE_SCHEMA_RE
        )
        if not allowed_schema.fullmatch(schema):
            expected = (
                "RAW_STAGE_<workflow_run_id>_<attempt>"
                if mode == "incremental"
                else "TORONTO_SNAPSHOT_REPLAY_<workflow_run_id>"
            )
            raise ValueError(f"Snowflake {mode} schema must be {expected}.")
        self.mode = mode
        self.credentials = credentials
        self.schema = schema
        self.input_stage_schema = snowflake_input_stage_schema(schema)
        (
            self._connection_values,
            self._key_bytes,
            self._passphrase,
        ) = self._parse_credentials(credentials)
        with self._session() as connection:
            cursor = connection.cursor()
            try:
                # Verify the account and execution context before any candidate DDL.
                cursor.execute(
                    "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_WAREHOUSE(), "
                    "CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME()"
                )
                _validate_snowflake_account_context(*cursor.fetchone())

                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS TORONTO."{self.schema}"')
                cursor.execute(
                    "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME=%s",
                    (self.schema,),
                )
                _validate_snowflake_schema_owner(self.schema, cursor.fetchone())
                cursor.execute(f'USE SCHEMA TORONTO."{self.schema}"')
                cursor.execute("SELECT CURRENT_SCHEMA()")
                current_schema = cursor.fetchone()[0]
                if str(current_schema).upper() != self.schema:
                    raise ValueError(
                        "Snowflake did not enter the requested replay schema: "
                        f"expected={self.schema}, actual={current_schema}."
                    )
                if mode == "incremental":
                    match = INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch(self.schema)
                    assert match is not None
                    expected_generation = f"{match.group(1)}:{match.group(2)}"
                    cursor.execute(
                        f'''SELECT GENERATION_ID, PROMOTION_STATE
                            FROM TORONTO."{self.schema}".TORONTO_CANDIDATE_CONTROL''',
                    )
                    control = cursor.fetchall()
                    if control != [(expected_generation, "READY")]:
                        raise ValueError(
                            "Incremental Snowflake loading requires the matching "
                            "prepared RAW candidate control marker; observed="
                            f"{control!r}. Run prepare_snowflake_raw_candidate.py first."
                        )
                connection.commit()
            finally:
                cursor.close()
        replay_warehouse_ledger.ensure_ledger(self)

    def completed_snapshot_ids(self) -> set[str]:
        return replay_warehouse_ledger.completed_snapshot_ids(self)

    def snapshot_status(self, snapshot_id: str, source_name: str) -> str | None:
        return replay_warehouse_ledger.snapshot_status(self, snapshot_id, source_name)

    def register_archive(self, archive: SnapshotArchive) -> None:
        replay_warehouse_ledger.register_archive(self, archive)

    def assert_can_load(self, archive: SnapshotArchive, source_name: str) -> None:
        replay_warehouse_ledger.assert_replay_order(self, archive, source_name)

    def set_snapshot_status(
        self, archive: SnapshotArchive, source_name: str, status: str, **kwargs: Any
    ) -> None:
        replay_warehouse_ledger.set_snapshot_status(
            self, archive, source_name, status, **kwargs
        )

    @staticmethod
    def _parse_credentials(
        credentials: str,
    ) -> tuple[dict[str, Any], bytes, str | None]:
        parts = urlsplit(credentials)
        params = parse_qs(parts.query)
        if parts.scheme != "snowflake" or not parts.hostname or not parts.username:
            raise ValueError(
                "Snowflake credentials must use the snowflake:// URI format."
            )
        if parts.hostname.lower() != EXPECTED_SNOWFLAKE_ACCOUNT:
            raise ValueError(
                "Snowflake replay credentials target an unexpected account."
            )
        if parts.path.strip("/").upper() != "TORONTO":
            raise ValueError(
                "Snowflake replay credentials must target the TORONTO database."
            )
        if params.get("role", [""])[0] != "TORONTO_INGEST_ROLE":
            raise ValueError("Snowflake replay requires TORONTO_INGEST_ROLE.")
        if params.get("warehouse", [""])[0] != "TORONTO_XS_WH":
            raise ValueError("Snowflake replay requires TORONTO_XS_WH.")
        encoded_key = params.get("private_key", [""])[0].replace(" ", "+")
        try:
            key_bytes = base64.b64decode(encoded_key, validate=True)
        except ValueError as exc:
            raise ValueError(
                "Snowflake private_key query value is not valid Base64."
            ) from exc
        if not key_bytes:
            raise ValueError("Snowflake private_key is empty.")
        values = {
            "account": parts.hostname,
            "user": unquote(parts.username),
            "role": "TORONTO_INGEST_ROLE",
            "warehouse": "TORONTO_XS_WH",
            "database": "TORONTO",
            "session_parameters": {"TIMEZONE": "UTC"},
            "login_timeout": 30,
        }
        passphrase = params.get("private_key_passphrase", [None])[0]
        return values, key_bytes, passphrase

    @contextmanager
    def _session(self) -> Iterator[Any]:
        import snowflake.connector

        with tempfile.TemporaryDirectory(
            prefix="toronto-snapshot-replay-key-"
        ) as directory:
            key_path = Path(directory) / "private_key.p8"
            key_path.write_bytes(self._key_bytes)
            key_path.chmod(0o600)
            options = {**self._connection_values, "private_key_file": str(key_path)}
            if self._passphrase:
                options["private_key_file_pwd"] = self._passphrase
            connection = snowflake.connector.connect(**options)
            try:
                yield connection
            finally:
                connection.close()

    def _current_scd2_rows(self, source_config: Any) -> int:
        """Count current rows in the RAW-derived candidate before this load."""
        table = _identifier(source_config.warehouse_table, "warehouse table").upper()
        with self._session() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f'SELECT COUNT(*) FROM TORONTO."{self.schema}"."{table}" '
                    'WHERE "_DLT_VALID_TO" IS NULL'
                )
                return int(cursor.fetchone()[0] or 0)
            finally:
                cursor.close()

    def _stage_input(
        self,
        archive: SnapshotArchive,
        source_name: str,
        parquet_path: Path,
        source_config: Any,
    ) -> dict[str, Any] | None:
        """Load and validate raw input in a separate Snowflake schema."""
        if self.mode != "incremental":
            return
        from toronto_housing_ingestion.dlt_loader import load_scd2

        stage_schema = self.input_stage_schema
        if not stage_schema or not SNOWFLAKE_INPUT_STAGE_SCHEMA_RE.fullmatch(
            stage_schema
        ):
            raise ValueError("Incremental Snowflake input-stage schema is invalid.")
        with self._session() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_WAREHOUSE(), "
                    "CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME()"
                )
                _validate_snowflake_account_context(*cursor.fetchone())
                cursor.execute(
                    "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME=%s",
                    (stage_schema,),
                )
                owner = cursor.fetchone()
                if owner is None:
                    cursor.execute(f'CREATE SCHEMA TORONTO."{stage_schema}"')
                    cursor.execute(
                        "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                        "WHERE SCHEMA_NAME=%s",
                        (stage_schema,),
                    )
                    owner = cursor.fetchone()
                if owner is None or str(owner[0]).upper() != "TORONTO_INGEST_ROLE":
                    raise ValueError(
                        f"Input-stage schema {stage_schema} must be owned by "
                        f"TORONTO_INGEST_ROLE; observed={owner!r}."
                    )
                connection.commit()
            finally:
                cursor.close()

        stage_config = _input_stage_config(source_config)
        load_scd2(
            credentials=self.credentials,
            snapshot_path=parquet_path,
            source=stage_config,
            dataset_name=stage_schema,
            destination_kind="snowflake",
            pipeline_name=f"toronto_input_stage_{source_name}",
        )
        table = _identifier(source_config.warehouse_table, "staging table").upper()
        with self._session() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f'SELECT COUNT(*) FROM TORONTO."{stage_schema}"."{table}"'
                )
                actual_rows = int(cursor.fetchone()[0] or 0)
                cursor.execute(
                    f'SELECT * FROM TORONTO."{stage_schema}"."{table}" LIMIT 0'
                )
                actual_columns = [column[0] for column in (cursor.description or [])]
                where, parameter = _warehouse_window_filter(
                    source_config, snowflake=True
                )
                cursor.execute(
                    f'SELECT COUNT(*) FROM TORONTO."{stage_schema}"."{table}"{where}',
                    (parameter,) if parameter is not None else None,
                )
                warehouse_rows = int(cursor.fetchone()[0] or 0)
            finally:
                cursor.close()
        source = archive.sources[source_name]
        _validate_staged_snapshot(
            source_name=source_name,
            expected_rows=source.row_count,
            expected_columns=source.columns,
            actual_rows=actual_rows,
            actual_columns=actual_columns,
        )
        quality = _analyse_mode1_snapshot(
            parquet_path,
            source_name,
            source_config,
            warehouse_rows=warehouse_rows,
            baseline_current_rows=self._current_scd2_rows(source_config),
        )
        return {
            "schema": stage_schema,
            "table": table,
            "rows": actual_rows,
            "quality": quality,
        }

    def load(
        self,
        archive: SnapshotArchive,
        source_name: str,
        parquet_path: Path,
        source_config: Any,
    ) -> dict[str, Any]:
        from toronto_housing_ingestion.dlt_loader import load_scd2

        input_stage = self._stage_input(
            archive, source_name, parquet_path, source_config
        )
        source = archive.sources[source_name]
        boundary = source_boundary(archive, source_name)
        result = load_scd2(
            credentials=self.credentials,
            snapshot_path=parquet_path,
            source=source_config,
            dataset_name=self.schema,
            destination_kind="snowflake",
            # Keep the canonical pipeline identity in both modes. The database/schema
            # is already isolated for Mode 2, so separate pipeline names are not
            # needed and would make a promoted rebuild incompatible with the next
            # routine production load.
            pipeline_name=PRODUCTION_PIPELINES[source_name],
            boundary_timestamp=boundary.isoformat(),
            snapshot_context={
                "snapshot_id": archive.snapshot_id,
                "source_run_id": archive.run_id,
                "source_run_attempt": archive.run_attempt,
                "extracted_at_utc": boundary.isoformat(),
                "source_commit": archive.source_commit,
                "release_tag": archive.release_tag,
                "asset_name": archive.asset_name,
                "parquet_sha256": source.parquet_sha256,
                "archive_sha256": archive.archive_sha256,
            },
        )
        _verify_scd2_quarantine_matches_preflight(
            source_name,
            (input_stage or {}).get("quality"),
            result,
        )
        return {
            "table": result.table_name,
            "quarantined_rows": result.quarantined_row_count,
            "quarantined_bad_rows": result.quarantined_bad_row_count,
            "quarantined_business_keys": result.quarantined_business_key_count,
            "quarantine_sha256": result.quarantine_sha256,
            "input_stage": input_stage,
        }

    def finalize(self) -> None:
        if self.mode != "incremental" or not self.input_stage_schema:
            return
        with self._session() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_WAREHOUSE(), "
                    "CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME()"
                )
                _validate_snowflake_account_context(*cursor.fetchone())
                cursor.execute(
                    "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME=%s",
                    (self.input_stage_schema,),
                )
                owner = cursor.fetchone()
                if owner is None:
                    return
                if str(owner[0]).upper() != "TORONTO_INGEST_ROLE":
                    raise ValueError(
                        f"Refusing to drop input-stage schema {self.input_stage_schema}: "
                        f"unexpected owner {owner[0]!r}."
                    )
                cursor.execute(
                    f'DROP SCHEMA TORONTO."{self.input_stage_schema}" CASCADE'
                )
                connection.commit()
            finally:
                cursor.close()

    def validate_and_export(
        self, source_configs: dict[str, Any], output_dir: Path
    ) -> dict[str, Any]:
        results: dict[str, Any] = {}
        with self._session() as connection:
            cursor = connection.cursor()
            try:
                for source_name, source in source_configs.items():
                    try:
                        table_name = _identifier(
                            source.warehouse_table, "warehouse table"
                        ).upper()
                        table = f'TORONTO."{self.schema}"."{table_name}"'
                        checks = validation_queries(
                            table, source.primary_key, snowflake=True
                        )
                        cursor.execute(checks["counts"])
                        total, current, expired = cursor.fetchone()
                        metrics = {
                            "total": int(total or 0),
                            "current": int(current or 0),
                            "expired": int(expired or 0),
                        }
                        for check, name in (
                            ("invalid", "invalid_intervals"),
                            ("overlap", "overlapping_intervals"),
                            ("duplicate", "duplicate_current_business_keys"),
                            ("null_keys", "null_business_keys"),
                        ):
                            cursor.execute(checks[check])
                            metrics[name] = int(cursor.fetchone()[0] or 0)
                        _validate_metrics(metrics)
                        output_dir.mkdir(parents=True, exist_ok=True)
                        cursor.execute(
                            f"{_source_export_sql(source, snowflake=True)} FROM {table}"
                        )
                        output = output_dir / f"{table_name.lower()}_history.csv.gz"
                        exported = _export_cursor(cursor, output)
                        if exported != metrics["total"]:
                            raise ValueError(
                                "History export row count does not match table count."
                            )
                        results[source_name] = {
                            "status": "passed",
                            **metrics,
                            "export": str(output),
                            "export_rows": exported,
                        }
                    except Exception as exc:
                        results[source_name] = {
                            "status": "failed",
                            "error": f"{type(exc).__name__}: {exc}"[:4000],
                        }
            finally:
                cursor.close()
        status = (
            "passed"
            if all(value["status"] == "passed" for value in results.values())
            else "failed"
        )
        return {"status": status, "sources": results}


def replay_backend(
    archives: list[SnapshotArchive],
    *,
    source_configs: dict[str, Any],
    target: Any,
    ledger_path: Path,
    staging_root: Path,
) -> dict[str, Any]:
    """Replay both source streams independently into one isolated backend."""
    if set(source_configs) != set(SOURCE_NAMES):
        raise ValueError(
            "Replay requires exactly the Active and Cleared source configurations."
        )
    ledger = ReplayLedger(ledger_path)
    if archives:
        ledger.register_archives(target.backend, archives)
        register_archive = getattr(target, "register_archive", None)
        if callable(register_archive):
            for archive in archives:
                register_archive(archive)
                for source_name in SOURCE_NAMES:
                    if (
                        target.snapshot_status(archive.snapshot_id, source_name)
                        == "succeeded"
                    ):
                        if (
                            ledger.status(
                                target.backend, source_name, archive.snapshot_id
                            )
                            != "succeeded"
                        ):
                            ledger.mark(
                                target.backend,
                                source_name,
                                archive.snapshot_id,
                                "loading",
                            )
                            ledger.mark(
                                target.backend,
                                source_name,
                                archive.snapshot_id,
                                "succeeded",
                            )
    staging_root.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "backend": target.backend,
        "status": "passed",
        "sources": {},
    }

    for source_name in SOURCE_NAMES:
        # Keep archive order compatible with the bundle-level production ledger.
        # load_scd2 still receives each source's own extraction-start boundary.
        ordered = sorted(archives, key=lambda item: item.order)
        stream: dict[str, Any] = {
            "attempted": 0,
            "succeeded": 0,
            "skipped": 0,
            "failed": [],
            "blocked": [],
            "loads": [],
        }
        for archive in ordered:
            status = ledger.status(target.backend, source_name, archive.snapshot_id)
            warehouse_status = getattr(target, "snapshot_status", lambda *_: None)(
                archive.snapshot_id, source_name
            )
            if status == "succeeded" or warehouse_status == "succeeded":
                if status != "succeeded":
                    ledger.mark(
                        target.backend, source_name, archive.snapshot_id, "loading"
                    )
                    ledger.mark(
                        target.backend, source_name, archive.snapshot_id, "succeeded"
                    )
                stream["skipped"] += 1
                continue
            try:
                ledger.assert_can_load(target.backend, source_name, archive.snapshot_id)
                assert_can_load = getattr(target, "assert_can_load", None)
                if callable(assert_can_load):
                    assert_can_load(archive, source_name)
            except Exception as exc:
                stream["blocked"].append(
                    {
                        "snapshot_id": archive.snapshot_id,
                        "reason": f"{type(exc).__name__}: {exc}"[:4000],
                    }
                )
                break

            ledger.mark(target.backend, source_name, archive.snapshot_id, "loading")
            set_snapshot_status = getattr(target, "set_snapshot_status", None)
            if callable(set_snapshot_status):
                set_snapshot_status(archive, source_name, "loading")
            stream["attempted"] += 1
            try:
                validate_source_contract(
                    archive, source_name, source_configs[source_name]
                )
                with tempfile.TemporaryDirectory(
                    prefix="toronto-snapshot-replay-", dir=staging_root
                ) as temp:
                    parquet_path = extract_source(archive, source_name, Path(temp))
                    result = target.load(
                        archive,
                        source_name,
                        parquet_path,
                        source_configs[source_name],
                    )
                if callable(set_snapshot_status):
                    set_snapshot_status(
                        archive, source_name, "succeeded", result=result
                    )
                ledger.mark(
                    target.backend, source_name, archive.snapshot_id, "succeeded"
                )
                stream["succeeded"] += 1
                stream["loads"].append(
                    {"snapshot_id": archive.snapshot_id, **(result or {})}
                )
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"[:4000]
                if callable(set_snapshot_status):
                    set_snapshot_status(archive, source_name, "failed", error=error)
                ledger.mark(
                    target.backend,
                    source_name,
                    archive.snapshot_id,
                    "failed",
                    error=error,
                )
                stream["failed"].append(
                    {"snapshot_id": archive.snapshot_id, "error": error}
                )
                break

        stream["status"] = (
            "passed" if not stream["failed"] and not stream["blocked"] else "failed"
        )
        report["sources"][source_name] = stream
        if stream["status"] != "passed":
            report["status"] = "failed"

    # Validate each source even when its sibling stream failed. This preserves
    # independent outcomes and still produces useful evidence for partial runs.
    report["history_validation"] = target.validate_and_export(
        source_configs, staging_root.parent / "exports"
    )
    if report["history_validation"].get("status") != "passed":
        report["status"] = "failed"

    if hasattr(target, "finalize"):
        target.finalize()
    return report
