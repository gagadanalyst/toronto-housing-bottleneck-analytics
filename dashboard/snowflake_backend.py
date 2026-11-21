"""Read dbt's shared dashboard-facing analytics contract with read-only access."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import pandas as pd

REQUIRED_RELATIONS = (
    "MART_ACTIVE_PERMIT_AGE",
    "MART_ACTIVE_BACKLOG_SUMMARY",
    "MART_ACTIVE_BACKLOG_BY_YEAR_TYPE",
    "MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE",
    "MART_CLEARED_LIFECYCLE_MATCHED_BY_YEAR_TYPE",
    "MART_CLEARED_LIFECYCLE_POOLED",
    "MART_DATA_QUALITY_SUMMARY",
)

REQUIRED_QUALITY_METRICS = (
    "active_revisions_analysed",
    "active_revisions_age_eligible",
    "active_revisions_age_ineligible",
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
    "active_exclusion_status_conflict",
    "active_exclusion_missing_status",
    "active_exclusion_terminal_or_exception_status",
    "active_exclusion_application_date_conflict",
    "active_exclusion_missing_application_date",
    "active_exclusion_unclassified",
    "cleared_revision_count",
    "builder_name_conflict",
    "has_status_conflict",
    "application_date_conflict",
    "issued_date_conflict",
    "completed_date_conflict",
    "geo_id_conflict",
    "application_to_issue_negative",
    "application_to_completion_negative",
    "issue_to_completion_negative",
    "application_date_usable",
    "application_date_no_usable",
    "issued_date_usable",
    "issued_date_no_usable",
    "completion_date_usable",
    "completion_date_no_usable",
    "both_dates_available",
    "closed_revision_count",
    "closed_matched_three_date_count",
    "closed_excluded_conflicting_date_count",
    "closed_excluded_missing_date_count",
    "closed_excluded_out_of_order_date_count",
    "closed_both_dates_available",
    "non_closed_both_dates_available",
    "cleared_builder_name_missing",
    "cleared_status_missing",
    "cleared_geo_id_missing",
    "cleared_revisions_with_findings",
)


def _identifier(value: str, label: str) -> str:
    """Validate and quote a Snowflake database or schema identifier."""
    normalized = value.strip().upper()
    if not re.fullmatch(r"[A-Z_][A-Z0-9_$]*", normalized):
        raise ValueError(f"Invalid Snowflake {label} identifier.")
    return f'"{normalized}"'


def _fetch_dataframe(cursor: Any, query: str) -> pd.DataFrame:
    """Execute a read query and return lowercase column names."""
    cursor.execute(query)
    columns = [str(column[0]).lower() for column in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def load_snowflake_dashboard_data(
    config: Mapping[str, Any],
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    int,
    dict[str, int],
    Any | None,
    Any | None,
]:
    """Read dbt marts only; all shared analytical calculations live in dbt."""
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
            key_text.encode(), password=password
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
        last_successful_source_extraction_utc = None
        if "DASHBOARD_REFRESH_METADATA" in available:
            cursor.execute(
                f"SELECT COLUMN_NAME FROM {database_sql}.INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (schema_sql.strip('"'), "DASHBOARD_REFRESH_METADATA"),
            )
            metadata_columns = {str(row[0]).upper() for row in cursor.fetchall()}
            refresh_columns = ["LAST_SUCCESSFUL_REFRESH_UTC"]
            if "LAST_SUCCESSFUL_SOURCE_EXTRACTION_UTC" in metadata_columns:
                refresh_columns.append("LAST_SUCCESSFUL_SOURCE_EXTRACTION_UTC")
            refresh = _fetch_dataframe(
                cursor,
                f"SELECT {', '.join(refresh_columns)} FROM {qualified_schema}.DASHBOARD_REFRESH_METADATA "
                "WHERE REFRESH_ID = 1",
            )
            if not refresh.empty:
                last_successful_refresh_utc = refresh.iloc[0][
                    "last_successful_refresh_utc"
                ]
                if "last_successful_source_extraction_utc" in refresh.columns:
                    last_successful_source_extraction_utc = refresh.iloc[0][
                        "last_successful_source_extraction_utc"
                    ]

        active = _fetch_dataframe(
            cursor, f"SELECT * FROM {qualified_schema}.MART_ACTIVE_PERMIT_AGE"
        )
        summary = _fetch_dataframe(
            cursor,
            f"SELECT * FROM {qualified_schema}.MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE",
        )
        active_backlog = _fetch_dataframe(
            cursor, f"SELECT * FROM {qualified_schema}.MART_ACTIVE_BACKLOG_SUMMARY"
        )
        active_backlog_by_year_type = _fetch_dataframe(
            cursor, f"SELECT * FROM {qualified_schema}.MART_ACTIVE_BACKLOG_BY_YEAR_TYPE"
        )
        matched_lifecycle = _fetch_dataframe(
            cursor,
            f"SELECT * FROM {qualified_schema}.MART_CLEARED_LIFECYCLE_MATCHED_BY_YEAR_TYPE",
        )
        pooled_lifecycle = _fetch_dataframe(
            cursor, f"SELECT * FROM {qualified_schema}.MART_CLEARED_LIFECYCLE_POOLED"
        )
        quality_frame = _fetch_dataframe(
            cursor,
            f"SELECT METRIC_NAME, METRIC_VALUE FROM {qualified_schema}.MART_DATA_QUALITY_SUMMARY",
        )

        for label, frame in (
            ("MART_ACTIVE_PERMIT_AGE", active),
            ("MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE", summary),
            ("MART_ACTIVE_BACKLOG_SUMMARY", active_backlog),
            ("MART_ACTIVE_BACKLOG_BY_YEAR_TYPE", active_backlog_by_year_type),
            ("MART_CLEARED_LIFECYCLE_MATCHED_BY_YEAR_TYPE", matched_lifecycle),
            ("MART_CLEARED_LIFECYCLE_POOLED", pooled_lifecycle),
            ("MART_DATA_QUALITY_SUMMARY", quality_frame),
        ):
            if frame.empty:
                raise RuntimeError(f"{qualified_schema}.{label} returned no records.")

        quality_counts = {
            str(row.metric_name): int(row.metric_value or 0)
            for row in quality_frame.itertuples(index=False)
        }
        missing_metrics = sorted(set(REQUIRED_QUALITY_METRICS) - set(quality_counts))
        if missing_metrics:
            raise RuntimeError(
                f"{qualified_schema}.MART_DATA_QUALITY_SUMMARY is missing metrics: "
                + ", ".join(missing_metrics)
            )
        cleared_revision_count = quality_counts["cleared_revision_count"]
        return (
            active,
            summary,
            active_backlog,
            active_backlog_by_year_type,
            matched_lifecycle,
            pooled_lifecycle,
            cleared_revision_count,
            quality_counts,
            last_successful_refresh_utc,
            last_successful_source_extraction_utc,
        )
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()
