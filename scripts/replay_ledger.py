"""Small SQLite ledger for independent backend/source snapshot replay."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SOURCE_NAMES = ("active_permits", "cleared_permits")
STATUSES = {"pending", "loading", "succeeded", "failed"}


class ReplayLedger:
    """Persist replay status independently for each backend and source.

    One ledger file belongs to one backend job. Its two source streams are
    independent, while each stream must advance in snapshot order.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS replay_loads (
                    backend TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    snapshot_id TEXT NOT NULL,
                    format_version INTEGER NOT NULL,
                    order_timestamp REAL NOT NULL,
                    run_id INTEGER NOT NULL,
                    run_attempt INTEGER NOT NULL,
                    source_commit TEXT NOT NULL,
                    archive_sha256 TEXT NOT NULL,
                    release_tag TEXT NOT NULL,
                    asset_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    updated_at_utc TEXT NOT NULL,
                    PRIMARY KEY (backend, source_name, snapshot_id),
                    CHECK (status IN ('pending', 'loading', 'succeeded', 'failed'))
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def register_archives(self, backend: str, archives: list[Any]) -> None:
        """Register immutable identities for both source streams in one transaction."""
        backend = self._backend(backend)
        now = datetime.now(UTC).isoformat()
        with self._connect() as connection:
            for archive in archives:
                _archive_timestamp, run_id, run_attempt, _snapshot_id = archive.order
                if set(archive.sources) != set(SOURCE_NAMES):
                    raise ValueError(
                        f"Archive {archive.snapshot_id} does not contain both sources."
                    )
                for source_name in SOURCE_NAMES:
                    source = archive.sources[source_name]
                    boundary = getattr(source, "extracted_at", archive.order[0])
                    identity = (
                        backend,
                        archive.format_version,
                        boundary.timestamp(),
                        int(run_id),
                        int(run_attempt),
                        archive.source_commit,
                        archive.archive_sha256,
                        archive.release_tag,
                        archive.asset_name,
                    )
                    existing = connection.execute(
                        """SELECT format_version, order_timestamp, run_id,
                                  run_attempt, source_commit, archive_sha256,
                                  release_tag, asset_name
                           FROM replay_loads
                           WHERE backend = ? AND source_name = ? AND snapshot_id = ?""",
                        (backend, source_name, archive.snapshot_id),
                    ).fetchone()
                    if existing is not None:
                        old_identity = tuple(existing)
                        new_identity = identity[1:]
                        if old_identity != new_identity:
                            raise ValueError(
                                "Replay ledger identity conflicts with immutable "
                                f"archive {backend}/{source_name}/"
                                f"{archive.snapshot_id}."
                            )
                        continue
                    connection.execute(
                        """INSERT INTO replay_loads (
                               backend, source_name, snapshot_id, format_version,
                               order_timestamp, run_id, run_attempt, source_commit,
                               archive_sha256, release_tag, asset_name, status,
                               error, updated_at_utc
                           ) VALUES (
                               ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?
                           )""",
                        (
                            backend,
                            source_name,
                            archive.snapshot_id,
                            *identity[1:],
                            now,
                        ),
                    )

    def status(self, backend: str, source_name: str, snapshot_id: str) -> str | None:
        backend = self._backend(backend)
        self._source(source_name)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT status FROM replay_loads "
                "WHERE backend = ? AND source_name = ? AND snapshot_id = ?",
                (backend, source_name, snapshot_id),
            ).fetchone()
        return str(row["status"]) if row else None

    def completed_snapshot_ids(self, backend: str) -> set[str]:
        """Return snapshots whose Active and Cleared streams both succeeded."""
        backend = self._backend(backend)
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT snapshot_id, COUNT(*) AS source_count,
                          SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS done
                   FROM replay_loads WHERE backend = ? GROUP BY snapshot_id
                   HAVING source_count = 2 AND done = 2""",
                (backend,),
            ).fetchall()
        return {str(row["snapshot_id"]) for row in rows}

    def snapshot_records(self, backend: str) -> list[dict[str, Any]]:
        """Return persisted archive identity and status for each source stream."""
        backend = self._backend(backend)
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT source_name, snapshot_id, format_version, order_timestamp,
                          run_id, run_attempt, source_commit, archive_sha256,
                          release_tag, asset_name, status
                   FROM replay_loads WHERE backend = ?
                   ORDER BY order_timestamp, run_id, run_attempt, snapshot_id,
                            source_name""",
                (backend,),
            ).fetchall()
        return [
            {
                "source_name": str(row["source_name"]),
                "snapshot_id": str(row["snapshot_id"]),
                "format_version": int(row["format_version"]),
                "order_timestamp": round(float(row["order_timestamp"]), 6),
                "run_id": str(row["run_id"]),
                "run_attempt": int(row["run_attempt"]),
                "source_commit": str(row["source_commit"]),
                "archive_sha256": str(row["archive_sha256"]),
                "release_tag": str(row["release_tag"]),
                "asset_name": str(row["asset_name"]),
                "status": str(row["status"]),
            }
            for row in rows
        ]

    def assert_can_load(self, backend: str, source_name: str, snapshot_id: str) -> None:
        """Reject gaps before this snapshot and writes already made after it."""
        backend = self._backend(backend)
        source_name = self._source(source_name)
        with self._connect() as connection:
            current = connection.execute(
                """SELECT order_timestamp, run_id, run_attempt, status
                   FROM replay_loads
                   WHERE backend = ? AND source_name = ? AND snapshot_id = ?""",
                (backend, source_name, snapshot_id),
            ).fetchone()
            if current is None:
                raise KeyError(
                    f"Snapshot is not registered: {backend}/{source_name}/{snapshot_id}"
                )
            if current["status"] == "succeeded":
                return
            current_order = (
                current["order_timestamp"],
                current["run_id"],
                current["run_attempt"],
                snapshot_id,
            )
            rows = connection.execute(
                """SELECT snapshot_id, order_timestamp, run_id, run_attempt, status
                   FROM replay_loads
                   WHERE backend = ? AND source_name = ?""",
                (backend, source_name),
            ).fetchall()

        earlier_incomplete: list[str] = []
        later_started: list[str] = []
        for row in rows:
            other_id = str(row["snapshot_id"])
            if other_id == snapshot_id:
                continue
            other_order = (
                row["order_timestamp"],
                row["run_id"],
                row["run_attempt"],
                other_id,
            )
            status = str(row["status"])
            if other_order < current_order and status != "succeeded":
                earlier_incomplete.append(f"{other_id}={status}")
            elif other_order > current_order and status != "pending":
                later_started.append(f"{other_id}={status}")
        if earlier_incomplete:
            raise RuntimeError(
                f"{backend}/{source_name} is blocked by earlier incomplete snapshots: "
                + ", ".join(sorted(earlier_incomplete))
            )
        if later_started:
            raise RuntimeError(
                f"Out-of-order replay rejected for {backend}/{source_name}; later "
                "snapshots may already have written: "
                + ", ".join(sorted(later_started))
            )

    def mark(
        self,
        backend: str,
        source_name: str,
        snapshot_id: str,
        status: str,
        *,
        error: str | None = None,
    ) -> None:
        backend = self._backend(backend)
        source_name = self._source(source_name)
        if status not in STATUSES:
            raise ValueError(f"Unsupported replay status: {status!r}.")
        now = datetime.now(UTC).isoformat()
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT status FROM replay_loads "
                "WHERE backend = ? AND source_name = ? AND snapshot_id = ?",
                (backend, source_name, snapshot_id),
            ).fetchone()
            if existing is None:
                raise KeyError(
                    f"Snapshot is not registered: {backend}/{source_name}/{snapshot_id}"
                )
            old_status = str(existing["status"])
            if old_status == "succeeded" and status != "succeeded":
                raise RuntimeError("A succeeded replay entry cannot be downgraded.")
            if status == "succeeded" and old_status not in {"loading", "succeeded"}:
                raise RuntimeError(
                    "A source must be marked loading before it can succeed."
                )
            connection.execute(
                """UPDATE replay_loads SET status = ?, error = ?, updated_at_utc = ?
                   WHERE backend = ? AND source_name = ? AND snapshot_id = ?""",
                (
                    status,
                    (error or "")[:4000] or None,
                    now,
                    backend,
                    source_name,
                    snapshot_id,
                ),
            )

    @staticmethod
    def _backend(value: str) -> str:
        backend = str(value).lower()
        if backend not in {"duckdb", "snowflake"}:
            raise ValueError(f"Unsupported replay backend: {value!r}.")
        return backend

    @staticmethod
    def _source(value: str) -> str:
        source = str(value)
        if source not in SOURCE_NAMES:
            raise ValueError(f"Unsupported replay source: {value!r}.")
        return source
