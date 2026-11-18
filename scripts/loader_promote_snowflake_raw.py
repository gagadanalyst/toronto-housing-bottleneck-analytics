"""Gate the legacy RAW candidate promoter for the new Snapshot Loader."""

from __future__ import annotations

from scripts.snowflake_operations import require_snowflake_operations_enabled


def main() -> int:
    require_snowflake_operations_enabled(
        "Promoting a Snapshot Loader Snowflake candidate"
    )

    # Import only after authorization so a disabled gate needs no Snowflake connector.
    from scripts.promote_snowflake_raw_candidate import main as promote_legacy_candidate

    result = promote_legacy_candidate()
    return int(result or 0)


if __name__ == "__main__":
    raise SystemExit(main())
