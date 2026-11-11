from __future__ import annotations

import duckdb

from dashboard.snowflake_backend import MATCHED_LIFECYCLE_POOLED_SQL


def test_pooled_lifecycle_query_uses_one_chronological_closed_cohort() -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """
            CREATE TABLE fct_cleared_permit_revision (
                status VARCHAR,
                application_date DATE,
                issued_date DATE,
                completed_date DATE,
                application_to_issue_days_raw BIGINT,
                application_to_completion_days_raw BIGINT,
                issue_to_completion_days_raw BIGINT
            )
            """
        )
        connection.execute(
            """
            INSERT INTO fct_cleared_permit_revision VALUES
                ('Closed', DATE '2024-01-01', DATE '2024-01-11', DATE '2024-04-10', 10, 100, 90),
                ('Closed', DATE '2024-02-01', DATE '2024-02-21', DATE '2024-08-19', 20, 200, 180),
                ('Closed', DATE '2024-03-01', DATE '2024-03-31', DATE '2024-12-26', 30, 300, 270),
                ('Active', DATE '2024-01-01', DATE '2024-01-11', DATE '2024-04-10', 10, 100, 90),
                ('Closed', DATE '2024-01-01', NULL, DATE '2024-04-10', NULL, NULL, NULL),
                ('Closed', DATE '2024-05-01', DATE '2024-04-01', DATE '2024-06-01', -30, 31, 61)
            """
        )

        result = connection.execute(
            MATCHED_LIFECYCLE_POOLED_SQL.format(
                fact_relation="fct_cleared_permit_revision"
            )
        ).fetchdf()

        assert result["measure"].tolist() == [
            "Application to issue",
            "Issue to completion",
            "Application to completion",
        ]
        assert result["matched_cohort_count"].tolist() == [3, 3, 3]
        assert result["median_days"].tolist() == [20.0, 180.0, 200.0]
        assert result["p90_days"].tolist() == [28.0, 252.0, 280.0]
    finally:
        connection.close()
