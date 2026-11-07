"""Compare a local DuckDB SCD2 history with the Snowflake replay exports."""

from __future__ import annotations

import argparse
import csv
import gzip
from collections import Counter
from pathlib import Path

import duckdb

DEFAULT_DATABASE = "data/recovery/scd2-rebuild/duckdb-replay/toronto-rebuilt.duckdb"
SOURCE_TABLES = {
    "active_permits": "raw_active_permits",
    "cleared_permits": "raw_cleared_permits",
}


def _schema_for_tables(connection: duckdb.DuckDBPyConnection) -> str:
    rows = connection.execute(
        "SELECT table_schema FROM information_schema.tables "
        "WHERE lower(table_name) IN ('raw_active_permits', 'raw_cleared_permits') "
        "GROUP BY table_schema "
        "HAVING COUNT(DISTINCT lower(table_name)) = 2"
    ).fetchall()
    if len(rows) != 1:
        raise ValueError(
            "Could not uniquely identify the DuckDB schema containing both permit tables; "
            "supply --dataset explicitly."
        )
    return str(rows[0][0])


def _duckdb_signatures(
    connection: duckdb.DuckDBPyConnection,
    dataset: str,
    table: str,
) -> Counter:
    dataset = dataset.upper()
    table = table.upper()
    if not dataset.replace("_", "").isalnum() or not table.replace("_", "").isalnum():
        raise ValueError("Unsafe DuckDB identifier.")
    rows = connection.execute(
        f"SELECT CAST(permit_num AS VARCHAR), CAST(revision_num AS VARCHAR), "
        f"CAST(permit_type AS VARCHAR), CAST(row_hash AS VARCHAR), "
        f"STRFTIME(_dlt_valid_from, '%Y-%m-%dT%H:%M:%S.%f'), "
        f"CASE WHEN _dlt_valid_to IS NULL THEN '' "
        f"ELSE STRFTIME(_dlt_valid_to, '%Y-%m-%dT%H:%M:%S.%f') END "
        f'FROM "{dataset}"."{table}"'
    ).fetchall()
    return Counter(
        tuple("" if value is None else str(value) for value in row) for row in rows
    )


def _snowflake_signatures(path: Path) -> Counter:
    with gzip.open(path, "rt", newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        required = {
            "permit_num",
            "revision_num",
            "permit_type",
            "row_hash",
            "valid_from",
            "valid_to",
        }
        fields = {name.lower(): name for name in (reader.fieldnames or [])}
        if set(fields) != required:
            raise ValueError(f"Unexpected Snowflake export columns in {path}.")
        result = Counter()
        for row in reader:
            result[
                (
                    row[fields["permit_num"]],
                    row[fields["revision_num"]],
                    row[fields["permit_type"]],
                    row[fields["row_hash"]],
                    row[fields["valid_from"]],
                    row[fields["valid_to"]],
                )
            ] += 1
        return result


def _first_examples(counter: Counter, limit: int = 3) -> list[tuple]:
    return list(counter.elements())[:limit]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path(DEFAULT_DATABASE))
    parser.add_argument(
        "--dataset", help="DuckDB schema; auto-detected if unambiguous."
    )
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        default=Path("data/recovery/scd2-rebuild/snowflake-replay-evidence"),
    )
    args = parser.parse_args()

    if not args.database.is_file():
        raise FileNotFoundError(args.database)
    with duckdb.connect(str(args.database), read_only=True) as connection:
        connection.execute("SET TimeZone='UTC'")
        dataset = args.dataset or _schema_for_tables(connection)
        print(f"DuckDB reference: {args.database} (schema={dataset})")
        failures = []
        for source_name, table in SOURCE_TABLES.items():
            duck_rows = _duckdb_signatures(connection, dataset, table)
            snowflake_path = args.evidence_dir / f"{table}_history.csv.gz"
            snowflake_rows = _snowflake_signatures(snowflake_path)
            missing = duck_rows - snowflake_rows
            extra = snowflake_rows - duck_rows
            duck_count = sum(duck_rows.values())
            snowflake_count = sum(snowflake_rows.values())
            print(
                f"{source_name}: DuckDB={duck_count:,}, Snowflake={snowflake_count:,}, "
                f"missing_from_Snowflake={sum(missing.values()):,}, "
                f"extra_in_Snowflake={sum(extra.values()):,}"
            )
            if missing or extra:
                failures.append(source_name)
                if missing:
                    print(
                        "  First DuckDB rows missing from Snowflake:",
                        _first_examples(missing),
                    )
                if extra:
                    print(
                        "  First Snowflake rows missing from DuckDB:",
                        _first_examples(extra),
                    )
            else:
                print(
                    f"PASS {source_name}: exact multiset parity across business keys, "
                    "row hashes and SCD2 validity boundaries."
                )

    if failures:
        raise SystemExit(f"SCD2 history parity failed for: {', '.join(failures)}")
    print("PASS: DuckDB and Snowflake SCD2 histories are exactly equivalent.")


if __name__ == "__main__":
    main()
