"""Gate Snapshot Loader Snowflake candidate preparation and cleanup."""

from __future__ import annotations

from scripts.snowflake_operations import require_snowflake_operations_enabled


def main() -> int:
    require_snowflake_operations_enabled(
        "Preparing or dropping a Snapshot Loader Snowflake candidate"
    )

    # Import only after authorization.
    from scripts.prepare_snowflake_raw_candidate import main as prepare_legacy_candidate

    result = prepare_legacy_candidate()
    return int(result or 0)


if __name__ == "__main__":
    raise SystemExit(main())
