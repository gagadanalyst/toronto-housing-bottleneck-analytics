"""Run the real Toronto extraction and dlt load twice in an isolated Snowflake schema.

The first pass uses the actual CLI and live CKAN source. Subsequent passes load
exactly the same extracted Parquet snapshots with --local-load and assert that
SCD2 total/current/expired counts do not change. A per-run schema is dropped at
the end so this gate cannot touch TORONTO.RAW or the shared SNOWFLAKE_SMOKE schema.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from datetime import UTC
from pathlib import Path

import dlt

from toronto_housing_ingestion.config import load_config


def _safe_dataset(value: str) -> str:
    """Accept only the dedicated per-run schema naming convention."""
    normalized = value.upper()
    if not re.fullmatch(r"SNOWFLAKE_INGEST_SMOKE_[A-Z0-9_]+", normalized):
        raise ValueError(
            "Refusing to run outside a per-run test schema named "
            "SNOWFLAKE_INGEST_SMOKE_<run-id>."
        )
    return normalized


def _count_rows(pipeline: dlt.Pipeline, table_name: str) -> tuple[int, int, int]:
    """Return (total, current, expired) counts for a loaded SCD2 table."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise ValueError(f"Unsafe table identifier: {table_name!r}")
    with pipeline.sql_client() as client:
        row = client.execute_sql(
            f"""
            SELECT
                COUNT(*),
                COUNT_IF(_dlt_valid_to IS NULL),
                COUNT_IF(_dlt_valid_to IS NOT NULL)
            FROM {table_name}
            """
        )[0]
    return int(row[0]), int(row[1] or 0), int(row[2] or 0)


def _run_cli(*arguments: str) -> None:
    """Run the project CLI in the current workflow checkout."""
    command = [sys.executable, "-m", "toronto_housing_ingestion.ingestion", *arguments]
    print("\n$ " + " ".join(command), flush=True)
    subprocess.run(command, check=True)


def _quote_identifier(value: str) -> str:
    """Quote a Snowflake identifier after checking its shape."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe Snowflake identifier: {value!r}")
    return f'"{value}"'


def _columns(client, schema: str, table: str) -> list[str]:
    """Read the ordered column list for one table in TORONTO."""
    schema = schema.upper()
    table = table.upper()
    if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", schema) or not re.fullmatch(
        r"[A-Z_][A-Z0-9_]*", table
    ):
        raise ValueError("Unsafe schema or table name for hash compatibility check.")
    rows = client.execute_sql(
        "SELECT COLUMN_NAME FROM TORONTO.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' "
        "ORDER BY ORDINAL_POSITION"
    )
    return [str(row[0]) for row in rows]


def _latest_parquet_snapshot(root: Path, source_name: str) -> Path | None:
    """Find the latest data.parquet snapshot for one source beneath root."""
    source_root = root / source_name
    candidates = list(source_root.glob("ingest_date=*/run_id=*/data.parquet"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


def _save_hash_mismatch_diagnostics(source, mismatch_rows: list[tuple]) -> Path:
    """Persist mismatch keys, complete typed snapshots, and field summaries."""
    import duckdb
    import pyarrow.parquet as pq

    if tuple(key.lower() for key in source.primary_key) != (
        "permit_num",
        "revision_num",
        "permit_type",
    ):
        raise ValueError(
            "Diagnostic capture currently expects the Active/Cleared permit key."
        )

    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    db_path = Path(
        os.environ.get(
            "HASH_MISMATCH_DIAGNOSTICS_PATH",
            "data/diagnostics/hash_mismatch_diagnostics.duckdb",
        )
    )
    db_path.parent.mkdir(parents=True, exist_ok=True)
    fresh_path = _latest_parquet_snapshot(Path("data/raw"), source.name)
    historical_root = Path(
        os.environ.get(
            "TORONTO_HISTORICAL_PARQUET_ROOT",
            "data/recovery/parquet-2026-10-23",
        )
    )
    historical_path = _latest_parquet_snapshot(historical_root, source.name)

    with duckdb.connect(str(db_path)) as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS hash_mismatches (
                run_id VARCHAR,
                source_name VARCHAR,
                permit_num VARCHAR,
                revision_num VARCHAR,
                permit_type VARCHAR,
                fresh_hash VARCHAR,
                migrated_hash VARCHAR,
                fresh_parquet_path VARCHAR,
                historical_parquet_path VARCHAR
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS diagnostic_runs (
                run_id VARCHAR,
                source_name VARCHAR,
                mismatch_join_rows BIGINT,
                distinct_business_keys BIGINT,
                fresh_parquet_path VARCHAR,
                historical_parquet_path VARCHAR,
                captured_at_utc TIMESTAMP
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS snapshot_schemas (
                run_id VARCHAR,
                source_name VARCHAR,
                snapshot_kind VARCHAR,
                column_name VARCHAR,
                parquet_type VARCHAR,
                nullable BOOLEAN
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS duplicate_keys (
                run_id VARCHAR,
                source_name VARCHAR,
                snapshot_kind VARCHAR,
                permit_num VARCHAR,
                revision_num VARCHAR,
                permit_type VARCHAR,
                physical_rows BIGINT
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS field_diff_summary (
                run_id VARCHAR,
                source_name VARCHAR,
                column_name VARCHAR,
                compared_row_pairs BIGINT,
                json_diff_row_pairs BIGINT,
                type_diff_row_pairs BIGINT
            )
        """)

        for table in (
            "hash_mismatches",
            "diagnostic_runs",
            "snapshot_schemas",
            "duplicate_keys",
            "field_diff_summary",
        ):
            db.execute(
                f"DELETE FROM {table} WHERE run_id = ? AND source_name = ?",
                [run_id, source.name],
            )

        # Each row returned from Snowflake is: three key values, fresh hash, migrated hash.
        mismatch_values = [
            (
                run_id,
                source.name,
                *(None if value is None else str(value) for value in row[:3]),
                str(row[3]),
                str(row[4]),
                str(fresh_path) if fresh_path else None,
                str(historical_path) if historical_path else None,
            )
            for row in mismatch_rows
        ]
        db.executemany(
            "INSERT INTO hash_mismatches VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            mismatch_values,
        )

        distinct_keys = len({tuple(row[:3]) for row in mismatch_rows})
        from datetime import datetime

        db.execute(
            "INSERT INTO diagnostic_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                run_id,
                source.name,
                len(mismatch_rows),
                distinct_keys,
                str(fresh_path) if fresh_path else None,
                str(historical_path) if historical_path else None,
                datetime.now(UTC).replace(tzinfo=None),
            ],
        )

        snapshot_tables: dict[str, tuple[str, Path]] = {}
        for snapshot_kind, path in (
            ("fresh", fresh_path),
            ("historical", historical_path),
        ):
            if path is None:
                print(
                    f"WARNING hash diagnostics: {snapshot_kind} Parquet not found "
                    f"for {source.name}; its snapshot values cannot be stored.",
                    flush=True,
                )
                continue
            table_name = f"{snapshot_kind}_{source.name}"
            db.execute(
                f'CREATE OR REPLACE TABLE "{table_name}" AS '
                "SELECT * FROM read_parquet(?)",
                [str(path)],
            )
            snapshot_tables[snapshot_kind] = (table_name, path)
            schema = pq.ParquetFile(path).schema_arrow
            db.executemany(
                "INSERT INTO snapshot_schemas VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        run_id,
                        source.name,
                        snapshot_kind,
                        field.name,
                        str(field.type),
                        bool(field.nullable),
                    )
                    for field in schema
                ],
            )

            key_map = {name.lower(): name for name in schema.names}
            if all(
                key in key_map for key in ("permit_num", "revision_num", "permit_type")
            ):
                qkeys = ", ".join(
                    f'"{key_map[key]}"'
                    for key in ("permit_num", "revision_num", "permit_type")
                )
                duplicate_rows = db.execute(
                    f"SELECT {qkeys}, COUNT(*) FROM read_parquet(?) "
                    f"GROUP BY {qkeys} HAVING COUNT(*) > 1",
                    [str(path)],
                ).fetchall()
                if duplicate_rows:
                    db.executemany(
                        "INSERT INTO duplicate_keys VALUES (?, ?, ?, ?, ?, ?, ?)",
                        [
                            (
                                run_id,
                                source.name,
                                snapshot_kind,
                                *(
                                    None if value is None else str(value)
                                    for value in row[:3]
                                ),
                                int(row[3]),
                            )
                            for row in duplicate_rows
                        ],
                    )

        # Compare the historical and fresh Parquet representations for keys that
        # failed Snowflake hash compatibility. Store a compact per-column summary.
        if "fresh" in snapshot_tables and "historical" in snapshot_tables:
            fresh_table = snapshot_tables["fresh"][0]
            historical_table = snapshot_tables["historical"][0]
            fresh_schema = pq.ParquetFile(fresh_path).schema_arrow
            historical_schema = pq.ParquetFile(historical_path).schema_arrow
            fresh_map = {name.lower(): name for name in fresh_schema.names}
            historical_map = {name.lower(): name for name in historical_schema.names}
            common = sorted(
                lower
                for lower in set(fresh_map) & set(historical_map)
                if lower != "_id" and not lower.startswith("_dlt_")
            )
            key_names = ("permit_num", "revision_num", "permit_type")
            common = [name for name in common if name not in key_names]
            selected_fields: list[str] = []
            value_tuples: list[str] = []
            for index, lower in enumerate(common):
                hname = historical_map[lower]
                fname = fresh_map[lower]
                selected_fields.extend(
                    [
                        f'h."{hname}" AS hist_{index}',
                        f'f."{fname}" AS fresh_{index}',
                    ]
                )
                value_tuples.append(
                    f"('{lower.upper()}', "
                    f"CAST(to_json(hist_{index}) AS VARCHAR), typeof(hist_{index}), "
                    f"CAST(to_json(fresh_{index}) AS VARCHAR), typeof(fresh_{index}))"
                )

            if selected_fields and value_tuples:
                analysis_sql = f"""
                    WITH mismatch_keys AS (
                        SELECT DISTINCT permit_num, revision_num, permit_type
                        FROM hash_mismatches
                        WHERE run_id = ? AND source_name = ?
                    ), matched_rows AS (
                        SELECT m.permit_num, m.revision_num, m.permit_type,
                               {", ".join(selected_fields)}
                        FROM mismatch_keys m
                        JOIN "{historical_table}" h ON
                            h."{historical_map["permit_num"]}" = m.permit_num AND
                            h."{historical_map["revision_num"]}" = m.revision_num AND
                            h."{historical_map["permit_type"]}" = m.permit_type
                        JOIN "{fresh_table}" f ON
                            f."{fresh_map["permit_num"]}" = m.permit_num AND
                            f."{fresh_map["revision_num"]}" = m.revision_num AND
                            f."{fresh_map["permit_type"]}" = m.permit_type
                    ), field_values AS (
                        SELECT d.column_name, d.historical_json, d.historical_type,
                               d.fresh_json, d.fresh_type
                        FROM matched_rows
                        CROSS JOIN LATERAL (VALUES {", ".join(value_tuples)}) AS d(
                            column_name, historical_json, historical_type,
                            fresh_json, fresh_type
                        )
                    )
                    INSERT INTO field_diff_summary
                    SELECT ?, ?, column_name, COUNT(*),
                           SUM(CASE WHEN historical_json IS DISTINCT FROM fresh_json
                                    THEN 1 ELSE 0 END),
                           SUM(CASE WHEN historical_type IS DISTINCT FROM fresh_type
                                    THEN 1 ELSE 0 END)
                    FROM field_values
                    GROUP BY column_name
                """
                db.execute(analysis_sql, [run_id, source.name, run_id, source.name])

    print(
        f"HASH DIAGNOSTICS SAVED: {db_path}; source={source.name}; "
        f"mismatch join rows={len(mismatch_rows):,}; "
        f"distinct business keys={distinct_keys:,}; "
        f"fresh Parquet={'found' if fresh_path else 'missing'}; "
        f"historical Parquet={'found' if historical_path else 'missing'}.",
        flush=True,
    )
    return db_path


def _verify_fresh_hash_compatibility(
    pipeline: dlt.Pipeline, dataset: str, source
) -> None:
    """Compare fresh hashes with migrated hashes for rows with identical data.

    The live smoke test already performs a full current CKAN extraction. This
    read-only comparison reuses that load and checks only rows whose tracked
    source attributes are identical in the fresh table and migrated TORONTO.RAW.
    Changed business values are intentionally excluded because their new hash is
    expected to differ. No production table is written.
    """
    table = source.warehouse_table.upper()
    dataset = dataset.upper()
    with pipeline.sql_client() as client:
        fresh_columns = _columns(client, dataset, table)
        migrated_columns = _columns(client, "RAW", table)
        fresh_map = {column.lower(): column for column in fresh_columns}
        migrated_map = {column.lower(): column for column in migrated_columns}

        required = {
            "row_hash",
            "_dlt_valid_to",
            *[key.lower() for key in source.primary_key],
        }
        for label, column_map in (("fresh", fresh_map), ("migrated", migrated_map)):
            missing = sorted(required - set(column_map))
            if missing:
                raise AssertionError(
                    f"{source.name}: {label} table is missing columns required for "
                    f"hash compatibility validation: {missing}"
                )

        def tracked_columns(column_map: dict[str, str]) -> dict[str, str]:
            return {
                lower: actual
                for lower, actual in column_map.items()
                if lower not in {"_id", "row_hash"} and not lower.startswith("_dlt_")
            }

        fresh_tracked = tracked_columns(fresh_map)
        migrated_tracked = tracked_columns(migrated_map)
        fresh_only = sorted(set(fresh_tracked) - set(migrated_tracked))
        migrated_only = sorted(set(migrated_tracked) - set(fresh_tracked))
        if fresh_only or migrated_only:
            raise AssertionError(
                f"{source.name}: cannot establish hash compatibility because source "
                "hash-input schemas differ between the fresh snapshot and migrated RAW. "
                f"Fresh-only columns={fresh_only}; migrated-only columns={migrated_only}. "
                "Review source schema drift before cutover."
            )

        key_columns = [key.lower() for key in source.primary_key]
        key_join = " AND ".join(
            f"fresh.{_quote_identifier(fresh_map[key])} = "
            f"migrated.{_quote_identifier(migrated_map[key])}"
            for key in key_columns
        )
        non_null_keys = " AND ".join(
            f"fresh.{_quote_identifier(fresh_map[key])} IS NOT NULL AND "
            f"migrated.{_quote_identifier(migrated_map[key])} IS NOT NULL"
            for key in key_columns
        )
        same_data = " AND ".join(
            f"fresh.{_quote_identifier(fresh_tracked[column])} IS NOT DISTINCT FROM "
            f"migrated.{_quote_identifier(migrated_tracked[column])}"
            for column in sorted(fresh_tracked)
        )
        fresh_hash = f"fresh.{_quote_identifier(fresh_map['row_hash'])}"
        migrated_hash = f"migrated.{_quote_identifier(migrated_map['row_hash'])}"
        fresh_valid_to = f"fresh.{_quote_identifier(fresh_map['_dlt_valid_to'])}"
        migrated_valid_to = (
            f"migrated.{_quote_identifier(migrated_map['_dlt_valid_to'])}"
        )
        join_and_filter = (
            f"FROM TORONTO.{dataset}.{table} AS fresh "
            f"JOIN TORONTO.RAW.{table} AS migrated ON {key_join} "
            f"WHERE {fresh_valid_to} IS NULL AND {migrated_valid_to} IS NULL "
            f"AND ({non_null_keys}) AND ({same_data})"
        )
        result = client.execute_sql(
            f"SELECT COUNT(*), COUNT_IF({fresh_hash} IS DISTINCT FROM {migrated_hash}) "
            f"{join_and_filter}"
        )[0]
        identical_rows = int(result[0] or 0)
        mismatched_hashes = int(result[1] or 0)

        if identical_rows == 0:
            raise AssertionError(
                f"{source.name}: no current rows had identical tracked attributes in the "
                "fresh snapshot and migrated RAW, so hash compatibility could not be tested."
            )
        if mismatched_hashes:
            key_projection = ", ".join(
                f"fresh.{_quote_identifier(fresh_map[key])}" for key in key_columns
            )
            mismatch_query = (
                f"SELECT {key_projection}, {fresh_hash}, {migrated_hash} "
                f"{join_and_filter} AND ({fresh_hash} IS DISTINCT FROM {migrated_hash})"
            )
            mismatch_rows = client.execute_sql(mismatch_query)
            try:
                _save_hash_mismatch_diagnostics(source, mismatch_rows)
            except Exception as diagnostic_error:
                # Never mask the original compatibility failure with a diagnostic error.
                print(
                    f"WARNING: unable to save hash mismatch diagnostics for "
                    f"{source.name}: {type(diagnostic_error).__name__}: "
                    f"{diagnostic_error}",
                    flush=True,
                )
            examples = mismatch_rows[:5]
            raise AssertionError(
                f"{source.name}: fresh-snapshot row hashes differ from migrated hashes for "
                f"{mismatched_hashes:,} of {identical_rows:,} current rows whose tracked "
                f"attributes are identical. Example (business key, fresh hash, migrated hash): "
                f"{examples!r}. This indicates a hash-input/type-normalization incompatibility."
            )

        print(
            f"PASS fresh-snapshot hash compatibility {source.name}: "
            f"{identical_rows:,} current rows with identical tracked attributes; "
            "zero hash mismatches against TORONTO.RAW.",
            flush=True,
        )


def main() -> None:
    """Extract live snapshots, replay the same files, and assert idempotence."""
    credentials = os.environ.get("TORONTO_WAREHOUSE_CREDENTIALS", "")
    kind = os.environ.get("TORONTO_WAREHOUSE_KIND", "").lower()
    if kind != "snowflake":
        raise SystemExit("Set TORONTO_WAREHOUSE_KIND=snowflake for this test.")
    if not credentials:
        raise SystemExit("TORONTO_WAREHOUSE_CREDENTIALS is not configured.")

    dataset = _safe_dataset(os.environ.get("TORONTO_WAREHOUSE_DATASET", ""))
    app = load_config(Path("open_data_config.yml"))
    sources = {source.name: source for source in app.sources}
    selected = ("active_permits", "cleared_permits")
    for name in selected:
        source = sources.get(name)
        if (
            source is None
            or not source.warehouse_load
            or source.warehouse_strategy != "scd2"
        ):
            raise RuntimeError(
                f"{name}: expected an enabled SCD2 source configuration."
            )

    # This probe pipeline only supplies a Snowflake SQL client. It does not run
    # any resources, and all queries are scoped to the isolated per-run schema.
    probe = dlt.pipeline(
        pipeline_name=f"toronto_ingestion_gate_probe_{dataset.lower()}",
        destination=dlt.destinations.snowflake(credentials=credentials),
        dataset_name=dataset,
        dev_mode=False,
    )

    try:
        # Perform a fresh, live source extraction and initial Snowflake load.
        source_args = tuple(arg for name in selected for arg in ("--source", name))
        _run_cli("--full-refresh", *source_args)

        baseline: dict[str, tuple[int, int, int]] = {}
        for name in selected:
            source = sources[name]
            counts = _count_rows(probe, source.warehouse_table)
            if counts[0] == 0 or counts[1] == 0:
                raise AssertionError(
                    f"{name}: first load produced no SCD2 rows: {counts}"
                )
            if counts[0] != counts[1] + counts[2]:
                raise AssertionError(f"{name}: SCD2 totals do not reconcile: {counts}")
            baseline[name] = counts
            print(
                f"BASELINE {name}: total={counts[0]:,}, current={counts[1]:,}, "
                f"expired={counts[2]:,}",
                flush=True,
            )
            _verify_fresh_hash_compatibility(probe, dataset, source)

        # Two loads of the identical Parquet snapshots: local-load reuses the
        # most recent successful extraction and does not make another CKAN call.
        for replay_number in (1, 2):
            _run_cli("--local-load", *source_args)
            for name in selected:
                source = sources[name]
                after = _count_rows(probe, source.warehouse_table)
                if after != baseline[name]:
                    raise AssertionError(
                        f"{name}: replay {replay_number} changed SCD2 counts. "
                        f"Baseline (total,current,expired)={baseline[name]}, "
                        f"after replay={after}."
                    )
                print(
                    f"PASS {name}: replay {replay_number} added no SCD2 versions; "
                    f"total={after[0]:,}, current={after[1]:,}, expired={after[2]:,}",
                    flush=True,
                )

        print(
            "SNOWFLAKE LIVE INGESTION SMOKE PASSED: real Active and Cleared "
            "extraction loaded to an isolated schema; two identical replays "
            "did not change SCD2 counts.",
            flush=True,
        )
    finally:
        # Drop only the schema whose name was validated above; never RAW.
        try:
            with probe.sql_client() as client:
                client.execute_sql(f"DROP SCHEMA IF EXISTS TORONTO.{dataset} CASCADE")
            print(f"Cleaned up isolated schema TORONTO.{dataset}.", flush=True)
        except Exception as exc:
            print(
                f"WARNING: unable to drop isolated schema TORONTO.{dataset}: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )


if __name__ == "__main__":
    main()
