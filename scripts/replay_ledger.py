"""Small SQLite ledger for independent backend/source snapshot replay."""

from __future__ import annotations

import os
import sqlite3
import tempfile
from contextlib import closing
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

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS replay_generations (
                    backend TEXT NOT NULL,
                    generation_id TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    history_scope TEXT NOT NULL,
                    candidate TEXT NOT NULL,
                    selected_snapshot_count INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('RUNNING', 'SUCCEEDED', 'FAILED')
                    ),
                    error TEXT,
                    started_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    PRIMARY KEY (backend, generation_id)
                )
                """
            )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS replay_bootstrap (
                    backend TEXT PRIMARY KEY,
                    generation_id TEXT NOT NULL,
                    candidate TEXT NOT NULL,
                    selected_snapshot_count INTEGER NOT NULL,
                    bootstrapped_at_utc TEXT NOT NULL
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

    def latest_successful_snapshot(self, backend: str) -> dict[str, Any] | None:
        """Return the newest archive whose Active and Cleared loads both succeeded.

        The result is backend-specific. A partially loaded archive is never
        eligible to become the row-count baseline.
        """
        backend = self._backend(backend)
        records = self.snapshot_records(backend)
        grouped: dict[str, dict[str, dict[str, Any]]] = {}

        for record in records:
            snapshot_id = str(record["snapshot_id"])
            source_name = str(record["source_name"])
            grouped.setdefault(snapshot_id, {})[source_name] = record

        identity_fields = (
            "format_version",
            "run_id",
            "run_attempt",
            "source_commit",
            "archive_sha256",
            "release_tag",
            "asset_name",
        )
        completed: list[dict[str, Any]] = []

        for snapshot_id, source_records in grouped.items():
            if set(source_records) != set(SOURCE_NAMES):
                continue
            if any(
                source_records[name]["status"] != "succeeded" for name in SOURCE_NAMES
            ):
                continue

            identities = {
                tuple(source_records[name][field] for field in identity_fields)
                for name in SOURCE_NAMES
            }
            if len(identities) != 1:
                raise RuntimeError(
                    "Successful source rows disagree on immutable archive identity "
                    f"for {backend}/{snapshot_id}."
                )

            result = dict(source_records[SOURCE_NAMES[0]])
            result["source_records"] = {
                name: dict(source_records[name]) for name in SOURCE_NAMES
            }
            result["order_timestamp"] = max(
                float(source_records[name]["order_timestamp"]) for name in SOURCE_NAMES
            )
            completed.append(result)

        if not completed:
            return None

        return max(
            completed,
            key=lambda item: (
                float(item["order_timestamp"]),
                int(item["run_id"]),
                int(item["run_attempt"]),
                str(item["snapshot_id"]),
            ),
        )

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

    @classmethod
    def bootstrap_working_ledger(
        cls,
        mode2_ledger_path: Path,
        working_ledger_path: Path,
        backend: str,
        generation_id: str,
    ) -> dict[str, Any]:
        """Create or verify a Mode 1 ledger copied from one accepted Mode 2 generation.

        The Mode 2 database is opened read-only and is never modified.
        An existing working ledger is reused only when its bootstrap identity
        and copied historical source records still match.
        """
        backend = cls._backend(backend)
        generation_id = str(generation_id).strip()
        source_path = Path(mode2_ledger_path).resolve()
        working_path = Path(working_ledger_path).resolve()

        if not generation_id:
            raise ValueError("Accepted Mode 2 generation ID must be non-empty.")
        if source_path == working_path:
            raise ValueError("Mode 2 and Mode 1 ledger paths must be different.")
        if not source_path.is_file():
            raise FileNotFoundError(f"Mode 2 ledger does not exist: {source_path}")

        source_uri = f"{source_path.as_uri()}?mode=ro"
        source = sqlite3.connect(source_uri, uri=True)
        source.row_factory = sqlite3.Row

        try:
            tables = {
                str(row["name"])
                for row in source.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            required = {"replay_loads", "replay_generations"}
            if not required.issubset(tables):
                raise RuntimeError("Mode 2 ledger is missing required replay tables.")

            generation = source.execute(
                """SELECT backend, generation_id, mode, history_scope,
                          candidate, selected_snapshot_count, status
                   FROM replay_generations
                   WHERE backend = ? AND generation_id = ?""",
                (backend, generation_id),
            ).fetchone()

            if generation is None:
                raise RuntimeError(
                    f"No Mode 2 generation record exists for {backend}/{generation_id}."
                )
            if generation["status"] != "SUCCEEDED":
                raise RuntimeError(
                    f"Mode 2 generation {backend}/{generation_id} is not SUCCEEDED."
                )
            if generation["mode"] != "rebuild" or generation["history_scope"] != "full":
                raise RuntimeError(
                    "Mode 1 requires a SUCCEEDED full-history Mode 2 rebuild."
                )

            candidate = str(generation["candidate"]).strip()
            selected_count = int(generation["selected_snapshot_count"])
            if not candidate or selected_count < 1:
                raise RuntimeError(
                    "Accepted Mode 2 generation has invalid candidate metadata."
                )

            columns = (
                "source_name, snapshot_id, format_version, order_timestamp, "
                "run_id, run_attempt, source_commit, archive_sha256, "
                "release_tag, asset_name, status"
            )
            source_rows = source.execute(
                f"""SELECT {columns}
                    FROM replay_loads
                    WHERE backend = ?
                    ORDER BY snapshot_id, source_name""",
                (backend,),
            ).fetchall()

            by_snapshot: dict[str, dict[str, str]] = {}
            for row in source_rows:
                snapshot_id = str(row["snapshot_id"])
                source_name = str(row["source_name"])
                if source_name not in SOURCE_NAMES:
                    raise RuntimeError(
                        f"Mode 2 ledger contains an unsupported source: {source_name}."
                    )
                source_statuses = by_snapshot.setdefault(snapshot_id, {})
                if source_name in source_statuses:
                    raise RuntimeError(
                        f"Mode 2 ledger has a duplicate source record for "
                        f"{snapshot_id}/{source_name}."
                    )
                source_statuses[source_name] = str(row["status"])

            if len(by_snapshot) != selected_count:
                raise RuntimeError(
                    f"Mode 2 generation expected {selected_count} snapshots, "
                    f"but its source ledger contains {len(by_snapshot)}."
                )

            for snapshot_id, statuses in by_snapshot.items():
                if set(statuses) != set(SOURCE_NAMES):
                    raise RuntimeError(
                        f"Mode 2 snapshot {snapshot_id} does not contain "
                        "exactly the Active and Cleared source records."
                    )
                if any(status != "succeeded" for status in statuses.values()):
                    raise RuntimeError(
                        f"Mode 2 snapshot {snapshot_id} has an unsuccessful source record."
                    )

            expected_rows = {
                (str(row["snapshot_id"]), str(row["source_name"])): tuple(row)
                for row in source_rows
            }

            def verify_existing_working_ledger() -> dict[str, Any]:
                working_uri = f"{working_path.as_uri()}?mode=ro"
                try:
                    with closing(sqlite3.connect(working_uri, uri=True)) as working:
                        working.row_factory = sqlite3.Row
                        marker = working.execute(
                            """SELECT backend, generation_id, candidate,
                                      selected_snapshot_count, bootstrapped_at_utc
                               FROM replay_bootstrap WHERE backend = ?""",
                            (backend,),
                        ).fetchone()
                        if marker is None:
                            raise RuntimeError(
                                "Existing Mode 1 working ledger has no bootstrap "
                                "identity; refusing to overwrite or adopt it."
                            )

                        identity = (
                            str(marker["generation_id"]),
                            str(marker["candidate"]),
                            int(marker["selected_snapshot_count"]),
                        )
                        expected_identity = (
                            generation_id,
                            candidate,
                            selected_count,
                        )
                        if identity != expected_identity:
                            raise RuntimeError(
                                "Existing Mode 1 working ledger belongs to a "
                                "different accepted Mode 2 generation."
                            )

                        existing_rows = working.execute(
                            f"""SELECT {columns}
                                FROM replay_loads
                                WHERE backend = ?
                                ORDER BY snapshot_id, source_name""",
                            (backend,),
                        ).fetchall()
                        actual_rows = {
                            (str(row["snapshot_id"]), str(row["source_name"])): tuple(
                                row
                            )
                            for row in existing_rows
                        }

                        for key, expected in expected_rows.items():
                            if actual_rows.get(key) != expected:
                                raise RuntimeError(
                                    "Mode 1 working ledger no longer matches "
                                    f"its Mode 2 baseline at {key[0]}/{key[1]}."
                                )

                        return {
                            "backend": backend,
                            "generation_id": generation_id,
                            "candidate": candidate,
                            "selected_snapshot_count": selected_count,
                            "bootstrapped_at_utc": str(marker["bootstrapped_at_utc"]),
                            "reused": True,
                        }
                except sqlite3.Error as exc:
                    raise RuntimeError(
                        "Existing Mode 1 working ledger cannot be verified."
                    ) from exc

            if working_path.exists():
                return verify_existing_working_ledger()

            working_path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{working_path.name}.",
                suffix=".tmp",
                dir=working_path.parent,
            )
            os.close(descriptor)
            temporary_path = Path(temporary_name)

            try:
                with closing(sqlite3.connect(temporary_path)) as destination:
                    source.backup(destination)
                    with destination:
                        destination.execute(
                            """CREATE TABLE IF NOT EXISTS replay_bootstrap (
                                backend TEXT PRIMARY KEY,
                                generation_id TEXT NOT NULL,
                                candidate TEXT NOT NULL,
                                selected_snapshot_count INTEGER NOT NULL,
                                bootstrapped_at_utc TEXT NOT NULL
                            )"""
                        )
                        bootstrapped_at = datetime.now(UTC).isoformat()
                        destination.execute(
                            """INSERT INTO replay_bootstrap (
                                   backend, generation_id, candidate,
                                   selected_snapshot_count, bootstrapped_at_utc
                               ) VALUES (?, ?, ?, ?, ?)""",
                            (
                                backend,
                                generation_id,
                                candidate,
                                selected_count,
                                bootstrapped_at,
                            ),
                        )

                if working_path.exists():
                    raise FileExistsError(
                        "Mode 1 working ledger appeared during bootstrap; "
                        "refusing to replace it."
                    )
                os.replace(temporary_path, working_path)
            finally:
                for temporary in (
                    temporary_path,
                    Path(f"{temporary_path}-wal"),
                    Path(f"{temporary_path}-shm"),
                ):
                    temporary.unlink(missing_ok=True)

            return {
                "backend": backend,
                "generation_id": generation_id,
                "candidate": candidate,
                "selected_snapshot_count": selected_count,
                "bootstrapped_at_utc": bootstrapped_at,
                "reused": False,
            }
        finally:
            source.close()

    def bootstrap_record(self, backend: str) -> dict[str, Any] | None:
        """Read the accepted Mode 2 identity used to initialize this ledger."""
        backend = self._backend(backend)
        with self._connect() as connection:
            row = connection.execute(
                """SELECT backend, generation_id, candidate,
                          selected_snapshot_count, bootstrapped_at_utc
                   FROM replay_bootstrap WHERE backend = ?""",
                (backend,),
            ).fetchone()
        return dict(row) if row is not None else None

    def start_generation(
        self,
        backend: str,
        generation_id: str,
        *,
        mode: str,
        history_scope: str,
        selected_snapshot_count: int,
        candidate: str,
    ) -> None:
        """Record that one backend's rebuild generation has started."""
        backend = self._backend(backend)
        generation_id = str(generation_id).strip()
        candidate = str(candidate).strip()

        if not generation_id or not candidate:
            raise ValueError("Generation ID and candidate must be non-empty.")
        if mode != "rebuild" or history_scope not in {"full", "partial"}:
            raise ValueError("Generation must describe a full or partial rebuild.")
        if type(selected_snapshot_count) is not int or selected_snapshot_count < 1:
            raise ValueError("Generation must select at least one snapshot.")

        now = datetime.now(UTC).isoformat()
        identity = (mode, history_scope, selected_snapshot_count, candidate)

        with self._connect() as connection:
            existing = connection.execute(
                """SELECT mode, history_scope, selected_snapshot_count,
                          candidate, status
                   FROM replay_generations
                   WHERE backend = ? AND generation_id = ?""",
                (backend, generation_id),
            ).fetchone()

            if existing is not None:
                if tuple(existing[:4]) != identity:
                    raise ValueError(
                        f"Generation identity changed for {backend}/{generation_id}."
                    )
                if existing["status"] == "SUCCEEDED":
                    raise RuntimeError("A succeeded generation cannot be restarted.")
                if existing["status"] == "RUNNING":
                    return

                connection.execute(
                    """UPDATE replay_generations
                       SET status = 'RUNNING', error = NULL,
                           started_at_utc = ?, updated_at_utc = ?
                       WHERE backend = ? AND generation_id = ?""",
                    (now, now, backend, generation_id),
                )
                return

            connection.execute(
                """INSERT INTO replay_generations (
                       backend, generation_id, mode, history_scope,
                       candidate, selected_snapshot_count, status,
                       error, started_at_utc, updated_at_utc
                   ) VALUES (?, ?, ?, ?, ?, ?, 'RUNNING', NULL, ?, ?)""",
                (
                    backend,
                    generation_id,
                    mode,
                    history_scope,
                    candidate,
                    selected_snapshot_count,
                    now,
                    now,
                ),
            )

    def finish_generation(
        self,
        backend: str,
        generation_id: str,
        status: str,
        *,
        error: str | None = None,
    ) -> None:
        """Set the terminal outcome after this backend's own validation."""
        backend = self._backend(backend)
        status = str(status).upper()
        if status not in {"SUCCEEDED", "FAILED"}:
            raise ValueError("Generation outcome must be SUCCEEDED or FAILED.")

        now = datetime.now(UTC).isoformat()
        with self._connect() as connection:
            row = connection.execute(
                """SELECT status FROM replay_generations
                   WHERE backend = ? AND generation_id = ?""",
                (backend, generation_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown generation: {backend}/{generation_id}.")

            old_status = str(row["status"])
            if old_status == status:
                return
            if old_status == "SUCCEEDED":
                raise RuntimeError("A succeeded generation cannot be downgraded.")
            if old_status != "RUNNING":
                raise RuntimeError("Only a running generation can be finalized.")

            connection.execute(
                """UPDATE replay_generations
                   SET status = ?, error = ?, updated_at_utc = ?
                   WHERE backend = ? AND generation_id = ?""",
                (
                    status,
                    (error or "")[:4000] or None,
                    now,
                    backend,
                    generation_id,
                ),
            )

    def generation_record(
        self, backend: str, generation_id: str
    ) -> dict[str, Any] | None:
        """Read one backend's generation outcome."""
        backend = self._backend(backend)
        with self._connect() as connection:
            row = connection.execute(
                """SELECT backend, generation_id, mode, history_scope,
                          candidate, selected_snapshot_count, status, error,
                          started_at_utc, updated_at_utc
                   FROM replay_generations
                   WHERE backend = ? AND generation_id = ?""",
                (backend, generation_id),
            ).fetchone()
        return dict(row) if row is not None else None

    def latest_successful_full_rebuild(self, backend: str) -> dict[str, Any] | None:
        """Return the latest successful full-history rebuild for this backend."""
        backend = self._backend(backend)
        with self._connect() as connection:
            row = connection.execute(
                """SELECT generation_id FROM replay_generations
                   WHERE backend = ? AND mode = 'rebuild'
                     AND history_scope = 'full' AND status = 'SUCCEEDED'
                   ORDER BY updated_at_utc DESC, generation_id DESC
                   LIMIT 1""",
                (backend,),
            ).fetchone()
        if row is None:
            return None
        return self.generation_record(backend, str(row["generation_id"]))

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
