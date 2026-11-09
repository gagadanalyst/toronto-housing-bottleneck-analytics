"""Atomically swap a validated Snowflake serving candidate into ANALYTICS."""

from __future__ import annotations

import argparse
import os
import re

import snowflake.connector

REQUIRED_RELATIONS = (
    "MART_ACTIVE_PERMIT_AGE",
    "FCT_ACTIVE_PERMIT_REVISION",
    "MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE",
    "FCT_CLEARED_PERMIT_REVISION",
    "DASHBOARD_REFRESH_METADATA",
)


def promote(stage: str) -> None:
    if not re.fullmatch(r"ANALYTICS_STAGE_[0-9]+_[0-9]+", stage):
        raise ValueError(f"Unsafe Snowflake serving candidate schema: {stage!r}.")
    required_env = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH")
    missing = [name for name in required_env if not os.environ.get(name)]
    if missing:
        raise ValueError(f"Missing Snowflake serving connection variables: {missing}.")
    connection = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role="TORONTO_DBT_ROLE",
        warehouse="TORONTO_XS_WH",
        database="TORONTO",
        schema="ANALYTICS",
    )
    cursor = connection.cursor()
    swapped = False
    try:
        cursor.execute(
            "SELECT SCHEMA_NAME, SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
            "WHERE SCHEMA_NAME IN ('ANALYTICS', %s)",
            (stage,),
        )
        owners = {
            str(name).upper(): str(owner).upper() for name, owner in cursor.fetchall()
        }
        if (
            owners.get("ANALYTICS") != "TORONTO_DBT_ROLE"
            or owners.get(stage) != "TORONTO_DBT_ROLE"
        ):
            raise RuntimeError(
                "TORONTO_DBT_ROLE must own both serving schemas before SWAP; "
                f"observed={owners}."
            )
        cursor.execute(f'ALTER SCHEMA TORONTO.ANALYTICS SWAP WITH TORONTO."{stage}"')
        swapped = True
        cursor.execute(
            "SELECT TABLE_NAME FROM TORONTO.INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = 'ANALYTICS'"
        )
        available = {str(row[0]).upper() for row in cursor.fetchall()}
        missing_relations = set(REQUIRED_RELATIONS) - available
        if missing_relations:
            raise RuntimeError(
                "Promoted ANALYTICS is missing dashboard relations: "
                f"{sorted(missing_relations)}."
            )
        for relation in REQUIRED_RELATIONS[:-1]:
            cursor.execute(f"SELECT COUNT(*) FROM TORONTO.ANALYTICS.{relation}")
            count = int(cursor.fetchone()[0])
            if count <= 0:
                raise RuntimeError(f"Promoted dashboard relation {relation} is empty.")
        cursor.execute(
            "SELECT COUNT(*) FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
            "WHERE SCHEMA_NAME = %s",
            (stage,),
        )
        if int(cursor.fetchone()[0]) != 1:
            raise RuntimeError("Previous ANALYTICS schema was not retained after SWAP.")
        print(
            f"SNOWFLAKE SERVING PROMOTION PASSED: candidate is now TORONTO.ANALYTICS; "
            f"previous serving schema is retained as TORONTO.{stage}."
        )
    except Exception:
        if swapped:
            try:
                cursor.execute(
                    f'ALTER SCHEMA TORONTO.ANALYTICS SWAP WITH TORONTO."{stage}"'
                )
                connection.commit()
                print(
                    "Serving post-swap verification failed; "
                    "restored previous ANALYTICS schema."
                )
            except Exception as rollback_error:
                raise RuntimeError(
                    "Serving promotion failed after schema swap and rollback failed: "
                    f"{rollback_error}"
                ) from rollback_error
        else:
            connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage-schema",
        default=os.environ.get("TORONTO_DBT_TARGET_SCHEMA", ""),
    )
    args = parser.parse_args()
    promote(args.stage_schema)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
