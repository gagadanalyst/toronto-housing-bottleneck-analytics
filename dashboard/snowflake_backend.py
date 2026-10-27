"""Read the dashboard-facing dbt marts from Snowflake."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import pandas as pd

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

REQUIRED_RELATIONS = (
    "MART_ACTIVE_PERMIT_AGE",
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
) -> tuple[pd.DataFrame, pd.DataFrame, int, dict[str, int]]:
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

        active = _fetch_dataframe(
            cursor,
            f"SELECT * FROM {qualified_schema}.MART_ACTIVE_PERMIT_AGE",
        )
        summary = _fetch_dataframe(
            cursor,
            f"SELECT * FROM {qualified_schema}.MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE",
        )

        flag_expressions = ", ".join(
            f"SUM(CASE WHEN COALESCE({field}, FALSE) THEN 1 ELSE 0 END) AS {field}"
            for field in FLAG_FIELDS
        )
        totals_frame = _fetch_dataframe(
            cursor,
            "SELECT COUNT(*) AS cleared_revision_count, "
            f"{flag_expressions} "
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
        cleared_flag_counts = {
            field: int(totals.get(field) or 0) for field in FLAG_FIELDS
        }

        return active, summary, cleared_revision_count, cleared_flag_counts
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()
