"""Create a clean run-specific Snowflake ANALYTICS dbt candidate schema."""

from __future__ import annotations

import argparse
import base64
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import snowflake.connector


def _connection_options(uri: str, key_path: Path) -> dict[str, str]:
    parts = urlsplit(uri)
    params = parse_qs(parts.query)
    if parts.scheme != "snowflake" or not parts.hostname or not parts.username:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is not a valid Snowflake URI.")
    if "private_key" not in params:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION has no private_key parameter.")
    encoded = params["private_key"][0].replace(" ", "+")
    key = base64.b64decode(encoded, validate=True)
    if not key:
        raise ValueError("Snowflake private key is empty.")
    key_path.write_bytes(key)
    key_path.chmod(0o600)
    return {
        "account": parts.hostname,
        "user": unquote(parts.username),
        "private_key_file": str(key_path),
        "role": "TORONTO_DBT_ROLE",
        "warehouse": "TORONTO_XS_WH",
        "database": "TORONTO",
        "schema": "ANALYTICS",
    }


def prepare(uri: str, run_id: str, attempt: str, github_env: Path | None) -> str:
    if not re.fullmatch(r"[0-9]+", run_id) or not re.fullmatch(r"[0-9]+", attempt):
        raise ValueError("GitHub run ID and attempt must contain digits only.")
    stage = f"ANALYTICS_STAGE_{run_id}_{attempt}"
    with tempfile.TemporaryDirectory(prefix="toronto-analytics-stage-") as temp:
        options = _connection_options(uri, Path(temp) / "snowflake_key.p8")
        connection = snowflake.connector.connect(**options)
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
            )
            role, database, schema = cursor.fetchone()
            if (
                str(role).upper() != "TORONTO_DBT_ROLE"
                or str(database).upper() != "TORONTO"
            ):
                raise RuntimeError(
                    f"Unexpected Snowflake dbt context: {role}/{database}/{schema}."
                )
            cursor.execute(
                "SELECT SCHEMA_NAME, SCHEMA_OWNER "
                "FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME IN ('RAW', 'ANALYTICS')"
            )
            owners = {
                str(name).upper(): str(owner).upper()
                for name, owner in cursor.fetchall()
            }
            if owners.get("ANALYTICS") != "TORONTO_DBT_ROLE":
                raise RuntimeError(
                    "TORONTO_DBT_ROLE must own canonical TORONTO.ANALYTICS before staging; "
                    f"observed={owners.get('ANALYTICS')!r}."
                )
            if owners.get("RAW") != "TORONTO_INGEST_ROLE":
                raise RuntimeError(
                    "Canonical TORONTO.RAW must remain owned by TORONTO_INGEST_ROLE; "
                    f"observed={owners.get('RAW')!r}."
                )
            cursor.execute(
                "SELECT COUNT(*) FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME = %s",
                (stage,),
            )
            if int(cursor.fetchone()[0] or 0):
                raise RuntimeError(
                    f"Candidate TORONTO.{stage} already exists. A workflow rerun must "
                    "use a new GitHub run attempt; stale candidates are never reused."
                )
            for table in ("RAW_ACTIVE_PERMITS", "RAW_CLEARED_PERMITS"):
                cursor.execute(
                    f"SELECT COUNT(*), COUNT_IF(_DLT_VALID_TO IS NULL) "
                    f"FROM TORONTO.RAW.{table}"
                )
                total, current = cursor.fetchone()
                if not total or not current:
                    raise RuntimeError(
                        f"TORONTO.RAW.{table} is missing or has no current rows: "
                        f"{total}/{current}."
                    )
            cursor.execute(f'CREATE SCHEMA TORONTO."{stage}"')
            cursor.execute(
                f'GRANT USAGE ON SCHEMA TORONTO."{stage}" '
                "TO ROLE TORONTO_DASHBOARD_ROLE"
            )
            cursor.execute(
                "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME = %s",
                (stage,),
            )
            owner = cursor.fetchone()
            if not owner or str(owner[0]).upper() != "TORONTO_DBT_ROLE":
                raise RuntimeError(
                    f"TORONTO_DBT_ROLE did not become owner of candidate {stage}: {owner}."
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    values = {
        "TORONTO_DBT_SOURCE_SCHEMA": "RAW",
        "TORONTO_DBT_TARGET_SCHEMA": stage,
        "TORONTO_DBT_SERVING_STAGE_SCHEMA": stage,
    }
    if github_env:
        with github_env.open("a", encoding="utf-8") as stream:
            for key, value in values.items():
                stream.write(f"{key}={value}\n")
    print(
        f"Prepared empty Snowflake serving candidate TORONTO.{stage}; "
        "production TORONTO.ANALYTICS was not modified."
    )
    return stage


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID", ""))
    parser.add_argument("--attempt", default=os.environ.get("GITHUB_RUN_ATTEMPT", "1"))
    parser.add_argument("--github-env", type=Path)
    args = parser.parse_args()
    uri = os.environ.get("SNOWFLAKE_DLT_CONNECTION", "")
    if not uri:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is required.")
    prepare(uri, args.run_id, args.attempt, args.github_env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
