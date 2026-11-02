"""Read the dashboard-facing dbt marts from Snowflake."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import pandas as pd

ACTIVE_FLAG_FIELDS = (
    "builder_name_conflict",
    "has_status_conflict",
    "application_date_conflict",
    "issued_date_conflict",
    "geo_id_conflict",
)

ACTIVE_AGGREGATE_FIELDS = (
    "active_builder_name_conflict",
    "active_has_status_conflict",
    "active_application_date_conflict",
    "active_issued_date_conflict",
    "active_geo_id_conflict",
    "active_builder_name_missing",
    "active_status_missing",
    "active_application_date_missing",
    "active_issued_date_missing",
    "active_geo_id_missing",
    "active_revisions_with_findings",
)

FLAG_FIELDS = (
    "builder_name_conflict",
    "has_status_conflict",
    "application_date_conflict",
    "issued_date_conflict",
    "completed_date_conflict",
    "geo_id_conflict",
    "application_to_issue_negative",
    "application_to_completion_negative",
    "issue_to_completion_negative",
)

DATE_COVERAGE_FIELDS = (
    "application_date_usable",
    "application_date_no_usable",
    "issued_date_usable",
    "issued_date_no_usable",
    "completion_date_usable",
    "completion_date_no_usable",
    "both_dates_available",
    "closed_revision_count",
    "closed_both_dates_available",
)

CLEARED_AGGREGATE_FIELDS = (
    "cleared_builder_name_missing",
    "cleared_status_missing",
    "cleared_geo_id_missing",
    "cleared_revisions_with_findings",
)

REQUIRED_RELATIONS = (
    "MART_ACTIVE_PERMIT_AGE",
    "FCT_ACTIVE_PERMIT_REVISION",
    "MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE",
    "FCT_CLEARED_PERMIT_REVISION",
)


def _identifier(value: str, label: str) -> str:
    """Validate and normalize a Snowflake database or schema identifier."""
    normalized = value.strip().upper()
    if not re.fullmatch(r"[A-Z_][A-Z0-9_$]*", normalized):
        raise ValueError(f"Invalid Snowflake {label} identifier.")
    return f'"{normalized}"'


def _fetch_dataframe(cursor: Any, query: str) -> pd.DataFrame:
    """Execute a read query and return its result with lowercase column names."""
    cursor.execute(query)
    columns = [str(column[0]).lower() for column in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def load_snowflake_dashboard_data(
    config: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, int, dict[str, int], Any | None]:
    """Read the three dashboard relations using a dedicated read-only role.

    Expected configuration keys:
      account, user, private_key
    Optional keys:
      private_key_password, warehouse, role, database, schema

    private_key must contain PEM-formatted private-key text. Store it in
    Streamlit secrets, never in source control.
    """
    required_settings = ("account", "user", "private_key")
    missing = [
        name for name in required_settings if not str(config.get(name, "")).strip()
    ]
    if missing:
        raise ValueError(
            "Snowflake dashboard configuration is missing: " + ", ".join(missing)
        )

    database_sql = _identifier(str(config.get("database", "TORONTO")), "database")
    schema_sql = _identifier(str(config.get("schema", "ANALYTICS")), "schema")
    warehouse = str(config.get("warehouse", "TORONTO_XS_WH")).strip()
    role = str(config.get("role", "TORONTO_DASHBOARD_ROLE")).strip()
    if not warehouse or not role:
        raise ValueError("Snowflake dashboard warehouse and role must be non-empty.")

    try:
        from cryptography.hazmat.primitives import serialization
        from snowflake import connector
    except ImportError as exc:
        raise RuntimeError(
            "Snowflake mode requires snowflake-connector-python."
        ) from exc

    key_text = str(config["private_key"]).replace("\\n", "\n").strip()
    password_text = config.get("private_key_password")
    password = str(password_text).encode() if password_text else None

    try:
        private_key = serialization.load_pem_private_key(
            key_text.encode(),
            password=password,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "The Snowflake dashboard private key could not be parsed. "
            "Check its PEM format and optional passphrase."
        ) from exc

    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    connection = connector.connect(
        account=str(config["account"]).strip(),
        user=str(config["user"]).strip(),
        private_key=private_key_der,
        warehouse=warehouse,
        role=role,
        database=database_sql.strip('"'),
        schema=schema_sql.strip('"'),
        login_timeout=15,
        network_timeout=120,
        client_session_keep_alive=False,
        application="toronto-housing-bottleneck-dashboard",
    )

    cursor = None
    try:
        cursor = connection.cursor()
        qualified_schema = f"{database_sql}.{schema_sql}"

        cursor.execute(
            f"SELECT TABLE_NAME FROM {database_sql}.INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = %s",
            (schema_sql.strip('"'),),
        )
        available = {str(row[0]).upper() for row in cursor.fetchall()}
        missing_relations = sorted(set(REQUIRED_RELATIONS) - available)
        if missing_relations:
            raise RuntimeError(
                f"Snowflake schema {schema_sql.strip(chr(34))} in "
                f"{database_sql.strip(chr(34))} is missing required dashboard "
                f"relations: {', '.join(missing_relations)}. "
                "Build and validate the dbt models in the configured schema."
            )

        last_successful_refresh_utc = None
        if "DASHBOARD_REFRESH_METADATA" in available:
            refresh_frame = _fetch_dataframe(
                cursor,
                f"SELECT LAST_SUCCESSFUL_REFRESH_UTC "
                f"FROM {qualified_schema}.DASHBOARD_REFRESH_METADATA "
                "WHERE REFRESH_ID = 1",
            )
            if not refresh_frame.empty:
                last_successful_refresh_utc = refresh_frame.iloc[0][
                    "last_successful_refresh_utc"
                ]

        active = _fetch_dataframe(
            cursor,
            f"SELECT * FROM {qualified_schema}.MART_ACTIVE_PERMIT_AGE",
        )
        summary = _fetch_dataframe(
            cursor,
            f"SELECT * FROM {qualified_schema}.MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE",
        )

        active_flag_expressions = [
            f"SUM(CASE WHEN COALESCE({field}, FALSE) THEN 1 ELSE 0 END) "
            f"AS active_{field}"
            for field in ACTIVE_FLAG_FIELDS
        ]
        active_missing_expressions = [
            "SUM(CASE WHEN builder_name IS NULL "
            "AND NOT COALESCE(builder_name_conflict, FALSE) "
            "THEN 1 ELSE 0 END) AS active_builder_name_missing",
            "SUM(CASE WHEN status IS NULL "
            "AND NOT COALESCE(has_status_conflict, FALSE) "
            "THEN 1 ELSE 0 END) AS active_status_missing",
            "SUM(CASE WHEN application_date IS NULL "
            "AND NOT COALESCE(application_date_conflict, FALSE) "
            "THEN 1 ELSE 0 END) AS active_application_date_missing",
            "SUM(CASE WHEN issued_date IS NULL "
            "AND NOT COALESCE(issued_date_conflict, FALSE) "
            "THEN 1 ELSE 0 END) AS active_issued_date_missing",
            "SUM(CASE WHEN geo_id IS NULL "
            "AND NOT COALESCE(geo_id_conflict, FALSE) "
            "THEN 1 ELSE 0 END) AS active_geo_id_missing",
        ]
        active_finding_conditions = [
            f"COALESCE({field}, FALSE)" for field in ACTIVE_FLAG_FIELDS
        ] + [
            "(status IS NULL AND NOT COALESCE(has_status_conflict, FALSE))",
            "(application_date IS NULL "
            "AND NOT COALESCE(application_date_conflict, FALSE))",
        ]
        active_missing_expressions.append(
            "SUM(CASE WHEN "
            + " OR ".join(active_finding_conditions)
            + " THEN 1 ELSE 0 END) AS active_revisions_with_findings"
        )
        active_quality_frame = _fetch_dataframe(
            cursor,
            "SELECT "
            + ", ".join(active_flag_expressions + active_missing_expressions)
            + f" FROM {qualified_schema}.FCT_ACTIVE_PERMIT_REVISION",
        )
        active_quality_counts = {
            field: int(active_quality_frame.iloc[0].get(field) or 0)
            for field in ACTIVE_AGGREGATE_FIELDS
        }

        flag_expressions = [
            f"SUM(CASE WHEN COALESCE({field}, FALSE) THEN 1 ELSE 0 END) AS {field}"
            for field in FLAG_FIELDS
        ]
        date_coverage_expressions = [
            "SUM(CASE WHEN application_date IS NOT NULL THEN 1 ELSE 0 END) "
            "AS application_date_usable",
            "SUM(CASE WHEN application_date IS NULL "
            "AND NOT COALESCE(application_date_conflict, FALSE) "
            "THEN 1 ELSE 0 END) AS application_date_no_usable",
            "SUM(CASE WHEN issued_date IS NOT NULL THEN 1 ELSE 0 END) "
            "AS issued_date_usable",
            "SUM(CASE WHEN issued_date IS NULL "
            "AND NOT COALESCE(issued_date_conflict, FALSE) "
            "THEN 1 ELSE 0 END) AS issued_date_no_usable",
            "SUM(CASE WHEN completed_date IS NOT NULL THEN 1 ELSE 0 END) "
            "AS completion_date_usable",
            "SUM(CASE WHEN completed_date IS NULL "
            "AND NOT COALESCE(completed_date_conflict, FALSE) "
            "THEN 1 ELSE 0 END) AS completion_date_no_usable",
            "SUM(CASE WHEN application_date IS NOT NULL "
            "AND completed_date IS NOT NULL THEN 1 ELSE 0 END) "
            "AS both_dates_available",
            "SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) "
            "AS closed_revision_count",
            "SUM(CASE WHEN status = 'Closed' "
            "AND application_date IS NOT NULL "
            "AND completed_date IS NOT NULL THEN 1 ELSE 0 END) "
            "AS closed_both_dates_available",
            "SUM(CASE WHEN observed_builder_names IS NULL "
            "AND NOT COALESCE(builder_name_conflict, FALSE) "
            "THEN 1 ELSE 0 END) AS cleared_builder_name_missing",
            "SUM(CASE WHEN status IS NULL "
            "AND NOT COALESCE(has_status_conflict, FALSE) "
            "THEN 1 ELSE 0 END) AS cleared_status_missing",
            "SUM(CASE WHEN geo_id IS NULL "
            "AND NOT COALESCE(geo_id_conflict, FALSE) "
            "THEN 1 ELSE 0 END) AS cleared_geo_id_missing",
            "SUM(CASE WHEN "
            + " OR ".join(
                [f"COALESCE({field}, FALSE)" for field in FLAG_FIELDS]
                + ["(status IS NULL AND NOT COALESCE(has_status_conflict, FALSE))"]
            )
            + " THEN 1 ELSE 0 END) AS cleared_revisions_with_findings",
        ]
        totals_frame = _fetch_dataframe(
            cursor,
            "SELECT COUNT(*) AS cleared_revision_count, "
            f"{', '.join(flag_expressions + date_coverage_expressions)} "
            f"FROM {qualified_schema}.FCT_CLEARED_PERMIT_REVISION",
        )

        if active.empty:
            raise RuntimeError(
                f"{qualified_schema}.MART_ACTIVE_PERMIT_AGE returned no records."
            )
        if summary.empty:
            raise RuntimeError(
                f"{qualified_schema}.MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE "
                "returned no records."
            )

        totals = totals_frame.iloc[0].to_dict()
        cleared_revision_count = int(totals.pop("cleared_revision_count") or 0)
        quality_counts = {
            field: int(totals.get(field) or 0)
            for field in (
                *FLAG_FIELDS,
                *DATE_COVERAGE_FIELDS,
                *CLEARED_AGGREGATE_FIELDS,
            )
        }
        quality_counts.update(active_quality_counts)

        return (
            active,
            summary,
            cleared_revision_count,
            quality_counts,
            last_successful_refresh_utc,
        )
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()
