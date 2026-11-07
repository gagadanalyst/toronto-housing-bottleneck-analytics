from itertools import islice

import duckdb
import pytest

from scripts.compare_duckdb_snowflake_parity import _compare_row_values

COLUMNS = ["permit_num", "revision_num", "permit_type", "status"]
KEYS = ("permit_num", "revision_num", "permit_type")


class FakeSnowflakeCursor:
    def __init__(self, rows):
        self.rows = iter(rows)

    def execute(self, query):
        return self

    def fetchmany(self, size=1000):
        return list(islice(self.rows, size))


def compare_rows(duck_rows, snowflake_rows):
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("CREATE SCHEMA analytics")
        connection.execute(
            """
            CREATE TABLE analytics.test_model (
                permit_num VARCHAR,
                revision_num VARCHAR,
                permit_type VARCHAR,
                status VARCHAR
            )
            """
        )
        if duck_rows:
            connection.executemany(
                "INSERT INTO analytics.test_model VALUES (?, ?, ?, ?)",
                duck_rows,
            )

        return _compare_row_values(
            duck_connection=connection,
            snowflake_cursor=FakeSnowflakeCursor(snowflake_rows),
            schema="PARITY_TEST",
            table="test_model",
            columns=COLUMNS,
            keys=KEYS,
        )
    finally:
        connection.close()


def test_identical_rows_pass():
    rows = [
        ("P-001", "00", "Building", "Issued"),
        ("P-002", "00", "Plumbing", "Closed"),
    ]

    result = compare_rows(rows, rows)

    assert result["values_match"] is True
    assert result["matched_key_count"] == 2
    assert result["duckdb_only_count"] == 0
    assert result["snowflake_only_count"] == 0
    assert result["value_mismatch_count"] == 0


@pytest.mark.parametrize(
    ("duck_rows", "snowflake_rows", "expected_reason", "expected_grain"),
    [
        (
            [
                ("P-001", "00", "Building", "Issued"),
                ("P-003", "00", "Building", "Closed"),
            ],
            [
                ("P-001", "00", "Building", "Issued"),
                ("P-002", "00", "Building", "Issued"),
                ("P-003", "00", "Building", "Closed"),
            ],
            "snowflake_only",
            {
                "permit_num": "P-002",
                "revision_num": "00",
                "permit_type": "Building",
            },
        ),
        (
            [
                ("P-001", "00", "Building", "Issued"),
                ("P-002", "00", "Building", "Issued"),
                ("P-003", "00", "Building", "Closed"),
            ],
            [
                ("P-001", "00", "Building", "Issued"),
                ("P-003", "00", "Building", "Closed"),
            ],
            "duckdb_only",
            {
                "permit_num": "P-002",
                "revision_num": "00",
                "permit_type": "Building",
            },
        ),
    ],
)
def test_missing_keys_are_identified_without_cascading(
    duck_rows,
    snowflake_rows,
    expected_reason,
    expected_grain,
):
    result = compare_rows(duck_rows, snowflake_rows)

    assert result["values_match"] is False
    assert result["matched_key_count"] == 2
    assert result["compared_rows"] == 3
    assert result["mismatches"] == [
        {
            "row_index": 2,
            "reason": expected_reason,
            "grain": expected_grain,
        }
    ]

    if expected_reason == "snowflake_only":
        assert result["snowflake_only_count"] == 1
        assert result["duckdb_only_count"] == 0
    else:
        assert result["duckdb_only_count"] == 1
        assert result["snowflake_only_count"] == 0


def test_attribute_difference_is_reported_for_matching_key():
    duck_rows = [
        ("P-001", "00", "Building", "Issued"),
    ]
    snowflake_rows = [
        ("P-001", "00", "Building", "Closed"),
    ]

    result = compare_rows(duck_rows, snowflake_rows)

    assert result["values_match"] is False
    assert result["matched_key_count"] == 1
    assert result["duckdb_only_count"] == 0
    assert result["snowflake_only_count"] == 0
    assert result["value_mismatch_count"] == 1
    assert result["mismatches"] == [
        {
            "row_index": 1,
            "reason": "value_mismatch",
            "grain": {
                "permit_num": "P-001",
                "revision_num": "00",
                "permit_type": "Building",
            },
            "column": "status",
            "duckdb": "Issued",
            "snowflake": "Closed",
        }
    ]
