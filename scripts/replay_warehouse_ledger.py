"""Persist replay progress in the candidate warehouse for the next daily run."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from scripts.replay_archive import SOURCE_NAMES, SnapshotArchive


def _boundary(archive: SnapshotArchive, source_name: str) -> datetime:
    """Return bundle start for ledger identity/order (not the SCD2 boundary)."""
    del source_name
    value = archive.extracted_at
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Snapshot source timestamp must include a timezone.")
    return parsed.astimezone(UTC)


def _values(archive: SnapshotArchive, source_name: str) -> tuple[Any, ...]:
    return (
        archive.run_id,
        archive.run_attempt,
        _boundary(archive, source_name),
        archive.source_commit,
        archive.release_tag,
        archive.asset_name,
        archive.format_version,
        archive.archive_sha256,
    )


def ensure_ledger(target: Any) -> None:
    """Create/upgrade the native ledger without changing the production schema."""
    if target.backend == "duckdb":
        import duckdb

        schema = target.dataset
        with duckdb.connect(str(target.database)) as connection:
            connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            connection.execute(
                f'''CREATE TABLE IF NOT EXISTS "{schema}".toronto_snapshot_ledger (
                    snapshot_id VARCHAR NOT NULL,
                    source_run_id VARCHAR NOT NULL,
                    source_run_attempt INTEGER NOT NULL,
                    extraction_started_at_utc TIMESTAMPTZ NOT NULL,
                    source_name VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    source_commit VARCHAR NOT NULL,
                    release_tag VARCHAR,
                    asset_name VARCHAR,
                    error VARCHAR,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    quarantined_row_count BIGINT DEFAULT 0,
                    quarantined_bad_row_count BIGINT DEFAULT 0,
                    quarantined_business_key_count BIGINT DEFAULT 0,
                    quarantine_sha256 VARCHAR,
                    format_version INTEGER,
                    archive_sha256 VARCHAR,
                    PRIMARY KEY (snapshot_id, source_name)
                )'''
            )
            for column, data_type in (
                ("format_version", "INTEGER"),
                ("archive_sha256", "VARCHAR"),
                ("quarantined_row_count", "BIGINT DEFAULT 0"),
                ("quarantined_bad_row_count", "BIGINT DEFAULT 0"),
                ("quarantined_business_key_count", "BIGINT DEFAULT 0"),
                ("quarantine_sha256", "VARCHAR"),
            ):
                connection.execute(
                    f'ALTER TABLE "{schema}".toronto_snapshot_ledger '
                    f'ADD COLUMN IF NOT EXISTS "{column}" {data_type}'
                )
        return

    with target._session() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                f'''CREATE TABLE IF NOT EXISTS TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER (
                    SNAPSHOT_ID VARCHAR NOT NULL,
                    SOURCE_RUN_ID VARCHAR NOT NULL,
                    SOURCE_RUN_ATTEMPT NUMBER(10,0) NOT NULL,
                    EXTRACTION_STARTED_AT_UTC TIMESTAMP_TZ NOT NULL,
                    SOURCE_NAME VARCHAR NOT NULL,
                    STATUS VARCHAR NOT NULL,
                    SOURCE_COMMIT VARCHAR NOT NULL,
                    RELEASE_TAG VARCHAR,
                    ASSET_NAME VARCHAR,
                    ERROR VARCHAR,
                    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                    QUARANTINED_ROW_COUNT NUMBER(38,0) DEFAULT 0,
                    QUARANTINED_BAD_ROW_COUNT NUMBER(38,0) DEFAULT 0,
                    QUARANTINED_BUSINESS_KEY_COUNT NUMBER(38,0) DEFAULT 0,
                    QUARANTINE_SHA256 VARCHAR,
                    FORMAT_VERSION NUMBER(10,0),
                    ARCHIVE_SHA256 VARCHAR(64)
                )'''
            )
            cursor.execute(
                f'''ALTER TABLE TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER
                    ADD COLUMN IF NOT EXISTS FORMAT_VERSION NUMBER(10,0)'''
            )
            cursor.execute(
                f'''ALTER TABLE TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER
                    ADD COLUMN IF NOT EXISTS ARCHIVE_SHA256 VARCHAR(64)'''
            )
            connection.commit()
        finally:
            cursor.close()


def _snapshot_order(snapshot_id: str, entries: list[tuple[Any, ...]]):
    """Return a consistent chronological identity for one snapshot."""
    identities = set()

    for row in entries:
        try:
            run_id = int(row[1])
            attempt = int(row[2])
            timestamp = row[3]

            if not isinstance(timestamp, datetime):
                timestamp = datetime.fromisoformat(
                    str(timestamp).replace("Z", "+00:00")
                )

            if timestamp.tzinfo is None:
                raise ValueError("Missing timezone.")

            timestamp = timestamp.astimezone(UTC)

        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Snapshot {snapshot_id} has invalid ledger identity metadata."
            ) from exc

        identities.add((timestamp, run_id, attempt))

    if len(identities) != 1:
        raise RuntimeError(f"Snapshot {snapshot_id} has conflicting source identities.")

    timestamp, run_id, attempt = next(iter(identities))

    # V1 snapshot IDs encode the originating run and attempt.
    parts = snapshot_id.rsplit(":", 1)
    if (
        len(parts) == 2
        and parts[0].isdigit()
        and parts[1].isdigit()
        and (int(parts[0]), int(parts[1])) != (run_id, attempt)
    ):
        raise RuntimeError(
            f"Snapshot {snapshot_id} conflicts with its ledger run identity."
        )

    return timestamp, run_id, attempt, snapshot_id


def validate_canonical_baseline(
    target: Any,
    baseline_snapshot_id: str,
    expected_snapshot_count: int,
) -> dict[str, Any]:
    """Verify the approved historical prefix before incremental replay."""
    if (
        not baseline_snapshot_id
        or isinstance(expected_snapshot_count, bool)
        or not isinstance(expected_snapshot_count, int)
        or expected_snapshot_count < 1
    ):
        raise ValueError("Invalid canonical baseline ID or snapshot count.")

    if target.backend == "duckdb":
        import duckdb

        with duckdb.connect(str(target.database), read_only=True) as connection:
            rows = connection.execute(
                f'''SELECT snapshot_id, source_run_id, source_run_attempt,
                           extraction_started_at_utc, source_name, status
                    FROM "{target.dataset}".toronto_snapshot_ledger'''
            ).fetchall()
    elif target.backend == "snowflake":
        with target._session() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f'''SELECT SNAPSHOT_ID, SOURCE_RUN_ID, SOURCE_RUN_ATTEMPT,
                               EXTRACTION_STARTED_AT_UTC, SOURCE_NAME, STATUS
                        FROM TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER'''
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()
    else:
        raise ValueError(f"Unsupported backend: {target.backend!r}.")

    grouped: dict[str, list[tuple[Any, ...]]] = {}
    for row in rows:
        grouped.setdefault(str(row[0]), []).append(tuple(row))

    if baseline_snapshot_id not in grouped:
        raise RuntimeError(
            f"Canonical baseline anchor {baseline_snapshot_id!r} is missing."
        )

    orders = {
        snapshot_id: _snapshot_order(snapshot_id, entries)
        for snapshot_id, entries in grouped.items()
    }
    anchor = orders[baseline_snapshot_id]

    prefix = sorted(order for order in orders.values() if order <= anchor)

    if len(prefix) != expected_snapshot_count or prefix[-1] != anchor:
        raise RuntimeError(
            f"Canonical baseline expected {expected_snapshot_count} snapshots "
            f"through {baseline_snapshot_id}; found {len(prefix)}."
        )

    expected_sources = set(SOURCE_NAMES)

    for order in prefix:
        snapshot_id = order[3]
        entries = grouped[snapshot_id]
        sources = [str(row[4]).lower() for row in entries]

        if len(entries) != 2 or set(sources) != expected_sources:
            raise RuntimeError(
                f"Snapshot {snapshot_id} must have exactly one entry "
                f"for each source; found {sources}."
            )

        if any(str(row[5]).lower() != "succeeded" for row in entries):
            raise RuntimeError(
                f"Both sources must have succeeded for snapshot {snapshot_id}."
            )

    return {
        "status": "passed",
        "baseline_snapshot_id": baseline_snapshot_id,
        "snapshot_count": expected_snapshot_count,
    }


def completed_snapshot_ids(target: Any) -> set[str]:
    if target.backend == "duckdb":
        import duckdb

        with duckdb.connect(str(target.database), read_only=True) as connection:
            rows = connection.execute(
                f'''SELECT snapshot_id
                    FROM "{target.dataset}".toronto_snapshot_ledger
                    GROUP BY snapshot_id
                    HAVING COUNT(DISTINCT source_name) = 2
                       AND COUNT_IF(LOWER(status) = 'succeeded') = 2'''
            ).fetchall()
        return {str(row[0]) for row in rows}

    with target._session() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                f'''SELECT SNAPSHOT_ID
                    FROM TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER
                    GROUP BY SNAPSHOT_ID
                    HAVING COUNT(DISTINCT SOURCE_NAME) = 2
                       AND COUNT_IF(LOWER(STATUS) = 'succeeded') = 2'''
            )
            return {str(row[0]) for row in cursor.fetchall()}
        finally:
            cursor.close()


def snapshot_status(target: Any, snapshot_id: str, source_name: str) -> str | None:
    if source_name not in SOURCE_NAMES:
        raise ValueError(f"Unsupported replay source: {source_name!r}.")
    if target.backend == "duckdb":
        import duckdb

        with duckdb.connect(str(target.database), read_only=True) as connection:
            row = connection.execute(
                f'''SELECT status FROM "{target.dataset}".toronto_snapshot_ledger
                    WHERE snapshot_id = ? AND source_name = ?''',
                [snapshot_id, source_name],
            ).fetchone()
        return str(row[0]).lower() if row else None

    with target._session() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                f'''SELECT STATUS FROM TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER
                    WHERE SNAPSHOT_ID=%s AND SOURCE_NAME=%s''',
                (snapshot_id, source_name),
            )
            row = cursor.fetchone()
            return str(row[0]).lower() if row else None
        finally:
            cursor.close()


def snapshot_records(target: Any) -> list[dict[str, Any]]:
    """Read persisted status and quarantine evidence from the candidate ledger."""
    if target.backend == "duckdb":
        import duckdb

        with duckdb.connect(str(target.database), read_only=True) as connection:
            rows = connection.execute(
                f"""SELECT snapshot_id, source_name, status,
                           COALESCE(quarantined_row_count, 0),
                           COALESCE(quarantined_bad_row_count, 0),
                           COALESCE(quarantined_business_key_count, 0),
                           quarantine_sha256
                    FROM "{target.dataset}".toronto_snapshot_ledger
                    ORDER BY snapshot_id, source_name"""
            ).fetchall()
    else:
        with target._session() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"""SELECT SNAPSHOT_ID, SOURCE_NAME, STATUS,
                               COALESCE(QUARANTINED_ROW_COUNT, 0),
                               COALESCE(QUARANTINED_BAD_ROW_COUNT, 0),
                               COALESCE(QUARANTINED_BUSINESS_KEY_COUNT, 0),
                               QUARANTINE_SHA256
                        FROM TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER
                        ORDER BY SNAPSHOT_ID, SOURCE_NAME"""
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()

    return [
        {
            "snapshot_id": str(row[0]),
            "source_name": str(row[1]),
            "status": str(row[2]).lower(),
            "quarantined_row_count": int(row[3]),
            "quarantined_bad_row_count": int(row[4]),
            "quarantined_business_key_count": int(row[5]),
            "quarantine_sha256": str(row[6]) if row[6] is not None else None,
        }
        for row in rows
    ]


def _assert_identity(old: tuple[Any, ...], values: tuple[Any, ...], label: str) -> None:
    old_time = old[2]
    if not isinstance(old_time, datetime):
        old_time = datetime.fromisoformat(str(old_time).replace("Z", "+00:00"))
    if old_time.tzinfo is None:
        raise ValueError(f"{label} ledger timestamp has no timezone.")
    old_time = old_time.astimezone(UTC)
    checks = (
        str(old[0]) == str(values[0]),
        int(old[1]) == int(values[1]),
        old_time == values[2],
        str(old[3]) == str(values[3]),
        old[4] in (None, values[4]),
        old[5] in (None, values[5]),
        old[7] in (None, values[6]),
        old[8] in (None, values[7]),
    )
    if not all(checks):
        raise ValueError(
            f"{label} ledger identity conflicts with the immutable archive."
        )


def register_archive(target: Any, archive: SnapshotArchive) -> None:
    """Register both sources before either is written; preserve prior successes."""
    if target.backend == "duckdb":
        import duckdb

        with duckdb.connect(str(target.database)) as connection:
            for source_name in SOURCE_NAMES:
                values = _values(archive, source_name)
                old = connection.execute(
                    f'''SELECT source_run_id, source_run_attempt,
                               extraction_started_at_utc, source_commit, release_tag,
                               asset_name, status, format_version, archive_sha256
                        FROM "{target.dataset}".toronto_snapshot_ledger
                        WHERE snapshot_id = ? AND source_name = ?''',
                    [archive.snapshot_id, source_name],
                ).fetchone()
                if old:
                    _assert_identity(tuple(old), values, "DuckDB")
                    connection.execute(
                        f'''UPDATE "{target.dataset}".toronto_snapshot_ledger
                            SET status=CASE WHEN LOWER(status)='succeeded'
                                            THEN status ELSE 'pending' END,
                                error=CASE WHEN LOWER(status)='succeeded'
                                           THEN error ELSE NULL END,
                                updated_at=CURRENT_TIMESTAMP,
                                release_tag=COALESCE(release_tag, ?),
                                asset_name=COALESCE(asset_name, ?),
                                format_version=COALESCE(format_version, ?),
                                archive_sha256=COALESCE(archive_sha256, ?)
                            WHERE snapshot_id=? AND source_name=?''',
                        [
                            archive.release_tag,
                            archive.asset_name,
                            archive.format_version,
                            archive.archive_sha256,
                            archive.snapshot_id,
                            source_name,
                        ],
                    )
                    continue
                connection.execute(
                    f'''INSERT INTO "{target.dataset}".toronto_snapshot_ledger
                        (snapshot_id, source_run_id, source_run_attempt,
                         extraction_started_at_utc, source_name, status, source_commit,
                         release_tag, asset_name, format_version, archive_sha256)
                        VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)''',
                    [
                        archive.snapshot_id,
                        archive.run_id,
                        archive.run_attempt,
                        values[2],
                        source_name,
                        archive.source_commit,
                        archive.release_tag,
                        archive.asset_name,
                        archive.format_version,
                        archive.archive_sha256,
                    ],
                )
        return

    with target._session() as connection:
        cursor = connection.cursor()
        try:
            for source_name in SOURCE_NAMES:
                values = _values(archive, source_name)
                cursor.execute(
                    f'''SELECT SOURCE_RUN_ID, SOURCE_RUN_ATTEMPT,
                               EXTRACTION_STARTED_AT_UTC, SOURCE_COMMIT, RELEASE_TAG,
                               ASSET_NAME, STATUS, FORMAT_VERSION, ARCHIVE_SHA256
                        FROM TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER
                        WHERE SNAPSHOT_ID=%s AND SOURCE_NAME=%s''',
                    (archive.snapshot_id, source_name),
                )
                old = cursor.fetchone()
                if old:
                    _assert_identity(tuple(old), values, "Snowflake")
                    cursor.execute(
                        f'''UPDATE TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER
                            SET STATUS=CASE WHEN LOWER(STATUS)='succeeded'
                                            THEN STATUS ELSE 'pending' END,
                                ERROR=CASE WHEN LOWER(STATUS)='succeeded'
                                           THEN ERROR ELSE NULL END,
                                UPDATED_AT=CURRENT_TIMESTAMP(),
                                RELEASE_TAG=COALESCE(RELEASE_TAG, %s),
                                ASSET_NAME=COALESCE(ASSET_NAME, %s),
                                FORMAT_VERSION=COALESCE(FORMAT_VERSION, %s),
                                ARCHIVE_SHA256=COALESCE(ARCHIVE_SHA256, %s)
                            WHERE SNAPSHOT_ID=%s AND SOURCE_NAME=%s''',
                        (
                            archive.release_tag,
                            archive.asset_name,
                            archive.format_version,
                            archive.archive_sha256,
                            archive.snapshot_id,
                            source_name,
                        ),
                    )
                    continue
                cursor.execute(
                    f'''INSERT INTO TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER
                        (SNAPSHOT_ID, SOURCE_RUN_ID, SOURCE_RUN_ATTEMPT,
                         EXTRACTION_STARTED_AT_UTC, SOURCE_NAME, STATUS, SOURCE_COMMIT,
                         RELEASE_TAG, ASSET_NAME, FORMAT_VERSION, ARCHIVE_SHA256)
                        VALUES (%s,%s,%s,%s,%s,'pending',%s,%s,%s,%s,%s)''',
                    (
                        archive.snapshot_id,
                        archive.run_id,
                        archive.run_attempt,
                        values[2],
                        source_name,
                        archive.source_commit,
                        archive.release_tag,
                        archive.asset_name,
                        archive.format_version,
                        archive.archive_sha256,
                    ),
                )
            connection.commit()
        finally:
            cursor.close()


def _assert_replay_order_rows(
    rows: list[tuple[Any, ...]],
    archive: SnapshotArchive,
    backend: str,
    source_name: str,
) -> None:
    """Check source chronology against persisted warehouse ledger rows."""
    current_order = (
        archive.extracted_at.astimezone(UTC),
        int(archive.run_id),
        int(archive.run_attempt),
        archive.snapshot_id,
    )
    earlier_incomplete: list[str] = []
    later_started: list[str] = []
    current_status: str | None = None
    for snapshot_id, run_id, attempt, extracted_at, status in rows:
        snapshot_key = str(snapshot_id)
        timestamp = extracted_at
        if not isinstance(timestamp, datetime):
            timestamp = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            raise ValueError(
                f"{backend}/{source_name} ledger timestamp has no timezone."
            )
        other_order = (
            timestamp.astimezone(UTC),
            int(run_id),
            int(attempt),
            snapshot_key,
        )
        normalized_status = str(status).lower()
        if snapshot_key == archive.snapshot_id:
            current_status = normalized_status
            continue
        if other_order < current_order and normalized_status != "succeeded":
            earlier_incomplete.append(f"{snapshot_key}={normalized_status}")
        elif other_order > current_order and normalized_status != "pending":
            later_started.append(f"{snapshot_key}={normalized_status}")

    if current_status == "succeeded":
        return
    if earlier_incomplete:
        raise RuntimeError(
            f"{backend}/{source_name} replay is blocked by earlier incomplete "
            "warehouse snapshots: " + ", ".join(sorted(earlier_incomplete))
        )
    if later_started:
        raise RuntimeError(
            f"Out-of-order {backend}/{source_name} replay rejected; later "
            "warehouse snapshots may already have written: "
            + ", ".join(sorted(later_started))
        )


def assert_replay_order(
    target: Any, archive: SnapshotArchive, source_name: str
) -> None:
    """Enforce chronology against the persistent candidate/production ledger."""
    if source_name not in SOURCE_NAMES:
        raise ValueError(f"Unsupported replay source: {source_name!r}.")

    if target.backend == "duckdb":
        import duckdb

        with duckdb.connect(str(target.database), read_only=True) as connection:
            rows = connection.execute(
                f"SELECT snapshot_id, source_run_id, source_run_attempt, "
                f"extraction_started_at_utc, status "
                f'FROM "{target.dataset}".toronto_snapshot_ledger '
                "WHERE source_name = ?",
                [source_name],
            ).fetchall()
    else:
        with target._session() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT SNAPSHOT_ID, SOURCE_RUN_ID, SOURCE_RUN_ATTEMPT, "
                    f"EXTRACTION_STARTED_AT_UTC, STATUS "
                    f'FROM TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER '
                    "WHERE SOURCE_NAME=%s"
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()
    _assert_replay_order_rows(rows, archive, target.backend, source_name)


def set_snapshot_status(
    target: Any,
    archive: SnapshotArchive,
    source_name: str,
    status: str,
    *,
    result: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    if source_name not in SOURCE_NAMES or status not in {
        "pending",
        "loading",
        "succeeded",
        "failed",
    }:
        raise ValueError("Invalid source or snapshot status.")
    result = result or {}
    metrics = (
        int(result.get("quarantined_rows", 0)),
        int(result.get("quarantined_bad_rows", 0)),
        int(result.get("quarantined_business_keys", 0)),
        result.get("quarantine_sha256"),
    )
    message = (error or "")[:4000] or None
    if target.backend == "duckdb":
        import duckdb

        with duckdb.connect(str(target.database)) as connection:
            connection.execute(
                f'''UPDATE "{target.dataset}".toronto_snapshot_ledger
                    SET status=?, error=?, updated_at=CURRENT_TIMESTAMP,
                        quarantined_row_count=?, quarantined_bad_row_count=?,
                        quarantined_business_key_count=?, quarantine_sha256=?
                    WHERE snapshot_id=? AND source_name=?''',
                [status, message, *metrics, archive.snapshot_id, source_name],
            )
            observed = connection.execute(
                f'''SELECT status FROM "{target.dataset}".toronto_snapshot_ledger
                    WHERE snapshot_id=? AND source_name=?''',
                [archive.snapshot_id, source_name],
            ).fetchone()
        if not observed or str(observed[0]).lower() != status:
            raise RuntimeError(
                "DuckDB warehouse snapshot ledger update did not persist."
            )
        return

    with target._session() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                f'''UPDATE TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER
                    SET STATUS=%s, ERROR=%s, UPDATED_AT=CURRENT_TIMESTAMP(),
                        QUARANTINED_ROW_COUNT=%s, QUARANTINED_BAD_ROW_COUNT=%s,
                        QUARANTINED_BUSINESS_KEY_COUNT=%s, QUARANTINE_SHA256=%s
                    WHERE SNAPSHOT_ID=%s AND SOURCE_NAME=%s''',
                (status, message, *metrics, archive.snapshot_id, source_name),
            )
            cursor.execute(
                f'''SELECT STATUS FROM TORONTO."{target.schema}".TORONTO_SNAPSHOT_LEDGER
                    WHERE SNAPSHOT_ID=%s AND SOURCE_NAME=%s''',
                (archive.snapshot_id, source_name),
            )
            observed = cursor.fetchone()
            if not observed or str(observed[0]).lower() != status:
                raise RuntimeError(
                    "Snowflake warehouse snapshot ledger update did not persist."
                )
            connection.commit()
        finally:
            cursor.close()
