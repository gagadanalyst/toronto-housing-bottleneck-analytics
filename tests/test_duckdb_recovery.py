from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from scripts import publish_duckdb_recovery as publisher
from scripts import restore_duckdb_recovery as restorer


def _database(path: Path) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA raw")
        connection.execute(
            """CREATE TABLE raw.toronto_snapshot_ledger (
                snapshot_id VARCHAR, source_run_id VARCHAR, source_run_attempt INTEGER,
                extraction_started_at_utc TIMESTAMPTZ, source_name VARCHAR,
                status VARCHAR, source_commit VARCHAR
            )"""
        )
        stamp = datetime(2026, 11, 7, 15, 0, tzinfo=UTC)
        connection.executemany(
            "INSERT INTO raw.toronto_snapshot_ledger VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("123:1", "123", 1, stamp, "active_permits", "succeeded", "a" * 40),
                ("123:1", "123", 1, stamp, "cleared_permits", "succeeded", "a" * 40),
            ],
        )


def test_publish_uses_month_of_latest_processed_snapshot_and_unique_asset_identity(
    tmp_path, monkeypatch
):
    database = tmp_path / "toronto.duckdb"
    _database(database)
    calls = []
    published_assets = {}
    monkeypatch.setattr(
        publisher, "_ensure_release", lambda repo, tag: calls.append(("release", tag))
    )

    def fake_publish_one(repo, tag, path, name):
        published_assets[name] = path.read_bytes()
        calls.append(("asset", tag, name, path.stat().st_size))

    def fake_run(*args):
        # The publisher downloads both assets for a final pair-integrity check.
        # Emulate the immutable Release contents created by fake_publish_one.
        assert args[0] == "release" and args[1] == "download"
        asset_name = args[args.index("--pattern") + 1]
        destination = Path(args[args.index("--dir") + 1])
        destination.mkdir(parents=True, exist_ok=True)
        if asset_name not in published_assets:
            raise AssertionError(f"Unexpected recovery asset download: {asset_name}")
        (destination / asset_name).write_bytes(published_assets[asset_name])
        return type("Result", (), {"stdout": ""})()

    monkeypatch.setattr(publisher, "_publish_one", fake_publish_one)
    monkeypatch.setattr(publisher, "_run", fake_run)

    result = publisher.publish_checkpoint(database, "owner/repo", "900", "2")

    assert result == (
        "toronto-duckdb-recovery-2026-11",
        "toronto-duckdb-recovery-900-2.duckdb",
    )
    assert [item[0] for item in calls] == ["release", "asset", "asset"]
    assert calls[1][2].endswith(".duckdb")
    assert calls[2][2].endswith(".json")
    assert len(published_assets) == 2


def test_restore_downloads_latest_versioned_pair_and_validates_database(
    tmp_path, monkeypatch
):
    source_db = tmp_path / "source.duckdb"
    _database(source_db)
    digest = publisher._sha256(source_db)
    metadata = {
        "format_version": 1,
        "checkpoint_run_id": "900",
        "checkpoint_run_attempt": 2,
        "latest_ledger_snapshot": {},
        "database_asset": "toronto-duckdb-recovery-900-2.duckdb",
        "database_size_bytes": source_db.stat().st_size,
        "database_sha256": digest,
        "release_tag": "toronto-duckdb-recovery-2026-11",
    }
    metadata_path = tmp_path / "toronto-duckdb-recovery-900-2.json"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    db_name = metadata["database_asset"]

    monkeypatch.setattr(
        restorer,
        "_available_checkpoints",
        lambda repository: [
            (
                datetime(2026, 11, 8, 1, 0, tzinfo=UTC),
                metadata["release_tag"],
                db_name,
                metadata_path.name,
            )
        ],
    )

    def fake_run(*args):
        # gh release download ... --dir <tempdir>; copy the named asset there.
        name = args[args.index("--pattern") + 1]
        target = Path(args[args.index("--dir") + 1])
        target.mkdir(parents=True, exist_ok=True)
        source = source_db if name == db_name else metadata_path
        (target / name).write_bytes(source.read_bytes())
        return type("Result", (), {"stdout": ""})()

    monkeypatch.setattr(restorer, "_run", fake_run)
    destination = tmp_path / "restored" / "toronto.duckdb"

    assert restorer.restore_latest("owner/repo", destination) is True
    assert restorer._sha256(destination) == digest
    with duckdb.connect(str(destination), read_only=True) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM raw.toronto_snapshot_ledger"
            ).fetchone()[0]
            == 2
        )


def test_restore_checksum_helper_reads_binary_stream_in_chunks(tmp_path):
    payload = tmp_path / "checksum.bin"
    payload.write_bytes(b"immutable-checkpoint" * 1000)
    assert restorer._sha256(payload) == publisher._sha256(payload)


def test_restore_rejects_duplicate_release_asset_names(monkeypatch):
    release_list = [
        {
            "tag_name": "toronto-duckdb-recovery-2026-11",
            "id": 42,
            "draft": False,
            "prerelease": False,
        }
    ]
    duplicate_assets = [
        {
            "name": "toronto-duckdb-recovery-900-1.duckdb",
            "created_at": "2026-11-08T00:00:00Z",
        },
        {
            "name": "toronto-duckdb-recovery-900-1.duckdb",
            "created_at": "2026-11-08T00:00:01Z",
        },
    ]

    def fake_run(*args):
        payload = (
            release_list if "releases?per_page=100" in args[-1] else duplicate_assets
        )
        return type("Result", (), {"stdout": json.dumps(payload)})()

    monkeypatch.setattr(restorer, "_run", fake_run)
    import pytest

    with pytest.raises(RuntimeError, match="duplicate asset names"):
        restorer._available_checkpoints("owner/repo")


def test_publish_refuses_recovery_checkpoint_from_partial_latest_snapshot(
    tmp_path, monkeypatch
):
    database = tmp_path / "partial.duckdb"
    _database(database)
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            "UPDATE raw.toronto_snapshot_ledger SET status = 'failed' "
            "WHERE source_name = 'cleared_permits'"
        )

    published = []
    monkeypatch.setattr(
        publisher, "_ensure_release", lambda *args: published.append(args)
    )

    with pytest.raises(
        RuntimeError, match="does not have exactly one succeeded ledger row"
    ):
        publisher.publish_checkpoint(database, "owner/repo", "901", "1")

    assert published == []
