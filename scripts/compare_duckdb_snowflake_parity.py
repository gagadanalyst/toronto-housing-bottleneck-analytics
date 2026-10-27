"""Compare same-snapshot DuckDB and Snowflake dbt outputs and write a JSON report."""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import duckdb
import snowflake.connector

MODELS = (
    "dim_date",
    "dim_permit_type",
    "stg_active_permits",
    "stg_cleared_permits",
    "fct_active_permit_revision",
    "fct_cleared_permit_revision",
    "mart_active_permit_age",
    "mart_cleared_lifecycle_by_year_type",
)
GRAINS = {
    "dim_date": ("date_day",),
    "dim_permit_type": ("permit_type",),
    "fct_active_permit_revision": ("permit_num", "revision_num", "permit_type"),
    "fct_cleared_permit_revision": ("permit_num", "revision_num", "permit_type"),
    "mart_active_permit_age": ("permit_num", "revision_num", "permit_type"),
    "mart_cleared_lifecycle_by_year_type": (
        "application_year",
        "permit_type",
        "measure",
    ),
}
ROW_COMPARE_MODELS = tuple(
    model
    for model in MODELS
    if model not in {"stg_active_permits", "stg_cleared_permits"}
)
FLOAT_TOLERANCE_COLUMNS = {"median_days", "p90_days", "negative_interval_rate_pct"}
NUMERIC_EPSILON = Decimal("0.000000001")
SUMMARY_TOLERANCE = Decimal("0.01")


def _semantic_type(type_name: str, numeric_scale: int | None = None) -> str:
    value = type_name.upper().strip()
    if "TIMESTAMP" in value or value.startswith("DATETIME"):
        return "timestamp"
    if value == "DATE":
        return "date"
    if value in {"BOOL", "BOOLEAN"}:
        return "boolean"
    if any(
        token in value for token in ("CHAR", "TEXT", "STRING", "VARCHAR", "VARIANT")
    ):
        return "text"
    if "BINARY" in value or value in {"BLOB", "BYTEA"}:
        return "binary"
    if value.startswith(("FLOAT", "DOUBLE", "REAL")):
        return "float"
    if value.startswith(("NUMBER", "DECIMAL", "NUMERIC")):
        scale = numeric_scale
        if scale is None:
            match = re.search(r"\((\d+)\s*,\s*(\d+)\)", value)
            scale = int(match.group(2)) if match else 0
        return "integer" if scale == 0 else "decimal"
    if value in {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "INT",
        "BIGINT",
        "HUGEINT",
        "UBIGINT",
        "UINTEGER",
        "USMALLINT",
        "UTINYINT",
    }:
        return "integer"
    return value.lower()


def _duckdb_columns(
    connection: duckdb.DuckDBPyConnection, table: str
) -> list[dict[str, str]]:
    rows = connection.execute(f'DESCRIBE SELECT * FROM analytics."{table}"').fetchall()
    return [
        {"name": str(row[0]).lower(), "type": _semantic_type(str(row[1]))}
        for row in rows
    ]


def _snowflake_columns(cursor: Any, schema: str, table: str) -> list[dict[str, str]]:
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_SCALE "
        "FROM TORONTO.INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
        "ORDER BY ORDINAL_POSITION",
        (schema.upper(), table.upper()),
    )
    return [
        {
            "name": str(row[0]).lower(),
            "type": _semantic_type(
                str(row[1]), int(row[2]) if row[2] is not None else None
            ),
        }
        for row in cursor.fetchall()
    ]


def _to_decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        try:
            result = Decimal(str(value))
            return result if result.is_finite() else None
        except InvalidOperation:
            return None
    return None


def _values_equal(left: Any, right: Any, column: str) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    left_number = _to_decimal(left)
    right_number = _to_decimal(right)
    if left_number is not None and right_number is not None:
        tolerance = (
            SUMMARY_TOLERANCE if column in FLOAT_TOLERANCE_COLUMNS else NUMERIC_EPSILON
        )
        return abs(left_number - right_number) <= tolerance
    if isinstance(left, (datetime, date, time)) and isinstance(
        right, (datetime, date, time)
    ):
        return left.isoformat() == right.isoformat()
    return left == right


def _row_stream(cursor: Any, batch_size: int = 1000):
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            return
        yield from batch


def _quote_duck_column(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _safe_count_duck(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    return int(
        connection.execute(f'SELECT COUNT(*) FROM analytics."{table}"').fetchone()[0]
    )


def _safe_count_snowflake(cursor: Any, schema: str, table: str) -> int:
    cursor.execute(f'SELECT COUNT(*) FROM TORONTO."{schema}"."{table.upper()}"')
    return int(cursor.fetchone()[0])


def _duplicate_grain_count_duck(
    connection: duckdb.DuckDBPyConnection, table: str, keys: tuple[str, ...]
) -> int:
    group = ", ".join(_quote_duck_column(key) for key in keys)
    return int(
        connection.execute(
            f'SELECT COUNT(*) FROM (SELECT {group} FROM analytics."{table}" '
            f"GROUP BY {group} HAVING COUNT(*) > 1) duplicates"
        ).fetchone()[0]
    )


def _duplicate_grain_count_snowflake(
    cursor: Any, schema: str, table: str, keys: tuple[str, ...]
) -> int:
    group = ", ".join(keys)
    cursor.execute(
        f'SELECT COUNT(*) FROM (SELECT {group} FROM TORONTO."{schema}"."{table.upper()}" '
        f"GROUP BY {group} HAVING COUNT(*) > 1) duplicates"
    )
    return int(cursor.fetchone()[0])


def _compare_row_values(
    duck_connection: duckdb.DuckDBPyConnection,
    snowflake_cursor: Any,
    schema: str,
    table: str,
    columns: list[str],
    keys: tuple[str, ...],
) -> dict:
    select_list = ", ".join(_quote_duck_column(column) for column in columns)
    order_by = ", ".join(_quote_duck_column(key) for key in keys)
    # dbt-created Snowflake identifiers are unquoted uppercase names. DuckDB
    # preserves lowercase model column names, so use unquoted identifiers for
    # the Snowflake projection and ordering (Snowflake resolves them uppercase).
    snowflake_select_list = ", ".join(columns)
    snowflake_order_by = ", ".join(keys)
    duck_cursor = duck_connection.execute(
        f'SELECT {select_list} FROM analytics."{table}" ORDER BY {order_by}'
    )
    snowflake_cursor.execute(
        f'SELECT {snowflake_select_list} FROM TORONTO."{schema}"."{table.upper()}" '
        f"ORDER BY {snowflake_order_by}"
    )
    key_positions = [columns.index(key) for key in keys]
    mismatches: list[dict] = []
    compared_rows = 0
    sentinel = object()
    duck_rows = _row_stream(duck_cursor)
    snowflake_rows = _row_stream(snowflake_cursor)
    while True:
        left = next(duck_rows, sentinel)
        right = next(snowflake_rows, sentinel)
        if left is sentinel and right is sentinel:
            break
        compared_rows += 1
        if left is sentinel or right is sentinel:
            if len(mismatches) < 10:
                mismatches.append(
                    {
                        "row_index": compared_rows,
                        "reason": "row present in only one backend",
                        "duckdb_present": left is not sentinel,
                        "snowflake_present": right is not sentinel,
                    }
                )
            continue
        key_value = {
            key: str(left[index])
            for key, index in zip(keys, key_positions, strict=True)
        }
        for name, left_value, right_value in zip(columns, left, right, strict=True):
            if not _values_equal(left_value, right_value, name):
                if len(mismatches) < 10:
                    mismatches.append(
                        {
                            "row_index": compared_rows,
                            "grain": key_value,
                            "column": name,
                            "duckdb": str(left_value),
                            "snowflake": str(right_value),
                        }
                    )
                break
    return {
        "compared_rows": compared_rows,
        "mismatches": mismatches,
        "values_match": not mismatches,
    }


def _active_summary(connection: Any, schema: str, table: str, duck: bool) -> dict:
    if duck:
        relation = f'analytics."{table}"'
        cur = connection.execute(
            f"SELECT COUNT(*), SUM(CASE WHEN is_age_eligible THEN 1 ELSE 0 END), "
            f"MIN(as_of_date), MAX(as_of_date), MIN(age_since_application_days), "
            f"MAX(age_since_application_days) FROM {relation}"
        )
        row = cur.fetchone()
        reason_rows = connection.execute(
            f"SELECT COALESCE(cohort_exclusion_reason, '<NULL>'), COUNT(*) "
            f"FROM {relation} GROUP BY 1 ORDER BY 1"
        ).fetchall()
    else:
        cur = connection.cursor()
        relation = f'TORONTO."{schema}"."{table.upper()}"'
        cur.execute(
            f"SELECT COUNT(*), SUM(CASE WHEN is_age_eligible THEN 1 ELSE 0 END), "
            f"MIN(as_of_date), MAX(as_of_date), MIN(age_since_application_days), "
            f"MAX(age_since_application_days) FROM {relation}"
        )
        row = cur.fetchone()
        cur.execute(
            f"SELECT COALESCE(cohort_exclusion_reason, '<NULL>'), COUNT(*) "
            f"FROM {relation} GROUP BY 1 ORDER BY 1"
        )
        reason_rows = cur.fetchall()
    return {
        "row_count": int(row[0] or 0),
        "eligible_count": int(row[1] or 0),
        "excluded_count": int((row[0] or 0) - (row[1] or 0)),
        "min_as_of_date": row[2].isoformat() if row[2] else None,
        "max_as_of_date": row[3].isoformat() if row[3] else None,
        "min_age_since_application_days": row[4],
        "max_age_since_application_days": row[5],
        "exclusion_reasons": {str(reason): int(count) for reason, count in reason_rows},
    }


def _cleared_summary(connection: Any, schema: str, table: str, duck: bool) -> dict:
    if duck:
        relation = f'analytics."{table}"'
        row = connection.execute(
            f"SELECT COUNT(*), COALESCE(SUM(eligible_date_pairs), 0), "
            f"COALESCE(SUM(negative_interval_count), 0), "
            f"COALESCE(SUM(publishable_duration_count), 0) FROM {relation}"
        ).fetchone()
        measure_rows = connection.execute(
            f"SELECT measure, COUNT(*), COALESCE(SUM(eligible_date_pairs), 0), "
            f"COALESCE(SUM(negative_interval_count), 0), "
            f"COALESCE(SUM(publishable_duration_count), 0) "
            f"FROM {relation} GROUP BY measure ORDER BY measure"
        ).fetchall()
    else:
        cursor = connection.cursor()
        relation = f'TORONTO."{schema}"."{table.upper()}"'
        cursor.execute(
            f"SELECT COUNT(*), COALESCE(SUM(eligible_date_pairs), 0), "
            f"COALESCE(SUM(negative_interval_count), 0), "
            f"COALESCE(SUM(publishable_duration_count), 0) FROM {relation}"
        )
        row = cursor.fetchone()
        cursor.execute(
            f"SELECT measure, COUNT(*), COALESCE(SUM(eligible_date_pairs), 0), "
            f"COALESCE(SUM(negative_interval_count), 0), "
            f"COALESCE(SUM(publishable_duration_count), 0) "
            f"FROM {relation} GROUP BY measure ORDER BY measure"
        )
        measure_rows = cursor.fetchall()
    return {
        "row_count": int(row[0] or 0),
        "eligible_date_pairs_sum": int(row[1] or 0),
        "negative_interval_count_sum": int(row[2] or 0),
        "publishable_duration_count_sum": int(row[3] or 0),
        "by_measure": {
            str(item[0]): {
                "row_count": int(item[1]),
                "eligible_date_pairs_sum": int(item[2] or 0),
                "negative_interval_count_sum": int(item[3] or 0),
                "publishable_duration_count_sum": int(item[4] or 0),
            }
            for item in measure_rows
        },
    }


def main() -> None:
    run_id = os.environ["SNAPSHOT_SOURCE_RUN_ID"]
    if not re.fullmatch(r"[0-9]+", run_id):
        raise ValueError("Snapshot source run ID must contain digits only.")
    schema = f"ANALYTICS_BASELINE_{run_id}"
    database_path = os.environ["DBT_DUCKDB_PATH"]
    connection_uri = os.environ.get("SNOWFLAKE_DLT_CONNECTION", "")
    if not connection_uri:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is required for parity comparison.")

    report: dict[str, Any] = {
        "snapshot_source_run_id": run_id,
        "snowflake_database": "TORONTO",
        "snowflake_target_schema": schema,
        "duckdb_target_schema": "analytics",
        "as_of_date": os.environ.get("TORONTO_DBT_AS_OF_DATE"),
        "numeric_tolerance": {
            "default_absolute": str(NUMERIC_EPSILON),
            "rounded_lifecycle_metrics_absolute": str(SUMMARY_TOLERANCE),
            "rounded_lifecycle_columns": sorted(FLOAT_TOLERANCE_COLUMNS),
        },
        "tables": {},
        "summaries": {},
        "overall_pass": False,
    }

    parts = urlsplit(connection_uri)
    if parts.scheme != "snowflake" or not parts.hostname:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is not a valid Snowflake URI.")
    snowflake_connection = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role="TORONTO_DBT_ROLE",
        warehouse="TORONTO_XS_WH",
        database="TORONTO",
    )
    duck_connection = duckdb.connect(database_path, read_only=True)
    try:
        cursor = snowflake_connection.cursor()
        try:
            all_pass = True
            for table in MODELS:
                table_result: dict[str, Any] = {
                    "duckdb_table": f"analytics.{table}",
                    "snowflake_table": f"TORONTO.{schema}.{table}",
                }
                duck_count = _safe_count_duck(duck_connection, table)
                snowflake_count = _safe_count_snowflake(cursor, schema, table)
                duck_columns = _duckdb_columns(duck_connection, table)
                snowflake_columns = _snowflake_columns(cursor, schema, table)
                duck_names = [column["name"] for column in duck_columns]
                snowflake_by_name = {
                    column["name"]: column["type"] for column in snowflake_columns
                }
                duck_by_name = {
                    column["name"]: column["type"] for column in duck_columns
                }
                same_columns = set(duck_by_name) == set(snowflake_by_name)
                same_types = same_columns and all(
                    duck_by_name[name] == snowflake_by_name[name]
                    for name in duck_by_name
                )
                grain = GRAINS.get(table)
                if grain:
                    duck_duplicates = _duplicate_grain_count_duck(
                        duck_connection, table, grain
                    )
                    snowflake_duplicates = _duplicate_grain_count_snowflake(
                        cursor, schema, table, grain
                    )
                    grain_result = {
                        "columns": list(grain),
                        "duckdb_duplicate_groups": duck_duplicates,
                        "snowflake_duplicate_groups": snowflake_duplicates,
                        "unique_in_both": duck_duplicates == 0
                        and snowflake_duplicates == 0,
                    }
                else:
                    grain_result = {"columns": None, "unique_in_both": None}

                values_result = None
                if table in ROW_COMPARE_MODELS and same_columns:
                    values_result = _compare_row_values(
                        duck_connection,
                        cursor,
                        schema,
                        table,
                        duck_names,
                        grain or (duck_names[0],),
                    )
                count_match = duck_count == snowflake_count
                table_pass = count_match and same_columns and same_types
                if grain:
                    table_pass = table_pass and bool(grain_result["unique_in_both"])
                if values_result is not None:
                    table_pass = table_pass and bool(values_result["values_match"])
                table_result.update(
                    {
                        "duckdb_row_count": duck_count,
                        "snowflake_row_count": snowflake_count,
                        "row_count_match": count_match,
                        "column_names_match": same_columns,
                        "semantic_column_types_match": same_types,
                        "duckdb_columns": duck_columns,
                        "snowflake_columns": snowflake_columns,
                        "grain": grain_result,
                        "row_values": values_result,
                        "pass": table_pass,
                    }
                )
                report["tables"][table] = table_result
                all_pass = all_pass and table_pass
                print(
                    f"{'PASS' if table_pass else 'DIFF'} {table}: "
                    f"DuckDB={duck_count:,}, Snowflake={snowflake_count:,}, "
                    f"columns={same_columns}, semantic_types={same_types}, "
                    f"row_values={'not compared' if values_result is None else values_result['values_match']}",
                    flush=True,
                )

            active_duck = _active_summary(
                duck_connection, "analytics", "mart_active_permit_age", True
            )
            active_snow = _active_summary(
                snowflake_connection, schema, "mart_active_permit_age", False
            )
            active_match = active_duck == active_snow
            report["summaries"]["mart_active_permit_age"] = {
                "duckdb": active_duck,
                "snowflake": active_snow,
                "match": active_match,
            }
            all_pass = all_pass and active_match

            cleared_duck = _cleared_summary(
                duck_connection,
                "analytics",
                "mart_cleared_lifecycle_by_year_type",
                True,
            )
            cleared_snow = _cleared_summary(
                snowflake_connection,
                schema,
                "mart_cleared_lifecycle_by_year_type",
                False,
            )
            cleared_match = cleared_duck == cleared_snow
            report["summaries"]["mart_cleared_lifecycle_by_year_type"] = {
                "duckdb": cleared_duck,
                "snowflake": cleared_snow,
                "match": cleared_match,
            }
            all_pass = all_pass and cleared_match
            report["overall_pass"] = all_pass
        finally:
            cursor.close()
    finally:
        duck_connection.close()
        snowflake_connection.close()

    output = Path(
        os.environ.get("PARITY_REPORT_PATH", "duckdb-snowflake-parity-report.json")
    )
    output.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote parity report: {output}", flush=True)
    if not report["overall_pass"]:
        raise SystemExit(
            "DuckDB/Snowflake parity comparison found differences; review the uploaded JSON report."
        )
    print("DUCKDB/SNOWFLAKE PARITY PASSED", flush=True)


if __name__ == "__main__":
    main()
