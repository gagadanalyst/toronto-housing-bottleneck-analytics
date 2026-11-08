import hashlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from zipfile import ZIP_STORED, ZipFile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import load_duckdb_snapshot as loader
from scripts import validate_duckdb_scd2_historical_replay as duckdb_validation
from scripts import validate_scd2_replay_provenance as provenance_validation


def _seed(
    db,
    *,
    run_id,
    status,
    timestamp="2026-11-07T15:00:00+00:00",
    source_name="active_permits",
):
    loader._record_state(
        db,
        snapshot_id=f"{run_id}:1",
        run_id=str(run_id),
        attempt=1,
        started=datetime.fromisoformat(timestamp),
        source_name=source_name,
        status=status,
        commit="a" * 40,
    )


def test_duckdb_ledger_blocks_newer_snapshot_when_older_one_is_incomplete(tmp_path):
    db = tmp_path / "ledger.duckdb"
    loader._ensure_ledger(db)
    _seed(db, run_id="100", status="failed")

    with pytest.raises(RuntimeError, match="earlier archived snapshots"):
        loader._assert_replay_order(
            db,
            snapshot_id="101:1",
            order=loader.snapshot_order("2026-11-07T15:01:00Z", "101", "1"),
        )


def test_duckdb_ledger_rejects_old_snapshot_after_newer_success(tmp_path):
    db = tmp_path / "ledger.duckdb"
    loader._ensure_ledger(db)
    _seed(db, run_id="101", status="succeeded", timestamp="2026-11-07T15:01:00+00:00")

    with pytest.raises(
        RuntimeError, match="newer snapshot has a non-pending source status"
    ):
        loader._assert_replay_order(
            db,
            snapshot_id="100:1",
            order=loader.snapshot_order("2026-11-07T15:00:00Z", "100", "1"),
        )


def test_duckdb_ledger_allows_retry_of_same_failed_snapshot(tmp_path):
    db = tmp_path / "ledger.duckdb"
    loader._ensure_ledger(db)
    _seed(db, run_id="100", status="failed")

    loader._assert_replay_order(
        db,
        snapshot_id="100:1",
        order=loader.snapshot_order("2026-11-07T15:00:00Z", "100", "1"),
    )


def test_duckdb_ledger_rows_are_per_source(tmp_path):
    db = tmp_path / "ledger.duckdb"
    loader._ensure_ledger(db)
    _seed(db, run_id="100", status="succeeded")
    with duckdb.connect(str(db), read_only=True) as connection:
        rows = connection.execute(
            "SELECT source_name, status FROM raw.toronto_snapshot_ledger"
        ).fetchall()
    assert rows == [("active_permits", "succeeded")]


def test_duckdb_ledger_allows_repeat_of_already_completed_old_archive(tmp_path):
    db = tmp_path / "ledger.duckdb"
    loader._ensure_ledger(db)
    for source_name in ("active_permits", "cleared_permits"):
        _seed(db, run_id="100", status="succeeded", source_name=source_name)
    _seed(db, run_id="101", status="succeeded", timestamp="2026-11-07T15:01:00+00:00")

    # A fully completed older identity is an idempotent no-op even after later loads.
    loader._assert_replay_order(
        db,
        snapshot_id="100:1",
        order=loader.snapshot_order("2026-11-07T15:00:00Z", "100", "1"),
    )


def test_duckdb_metadata_table_migrates_early_patch_schema(tmp_path):
    db = tmp_path / "legacy-metadata.duckdb"
    with duckdb.connect(str(db)) as connection:
        connection.execute("CREATE SCHEMA raw")
        connection.execute(
            "CREATE TABLE raw.toronto_source_snapshot_metadata ("
            "source_name VARCHAR PRIMARY KEY, extracted_at_utc TIMESTAMPTZ NOT NULL, "
            "manifest_json VARCHAR NOT NULL, updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)"
        )

    loader._ensure_ledger(db)

    with duckdb.connect(str(db), read_only=True) as connection:
        columns = {
            row[0]
            for row in connection.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='raw' AND table_name='toronto_source_snapshot_metadata'"
            ).fetchall()
        }
    assert {"release_tag", "asset_name"}.issubset(columns)


def test_duckdb_ledger_blocks_newer_snapshot_when_older_source_row_is_missing(tmp_path):
    db = tmp_path / "ledger.duckdb"
    loader._ensure_ledger(db)
    _seed(db, run_id="100", status="succeeded", source_name="active_permits")

    with pytest.raises(RuntimeError, match="both source loads recorded as succeeded"):
        loader._assert_replay_order(
            db,
            snapshot_id="101:1",
            order=loader.snapshot_order("2026-11-07T15:01:00Z", "101", "1"),
        )


@pytest.mark.parametrize("later_status", ["loading", "failed"])
def test_duckdb_ledger_blocks_old_replay_when_newer_write_may_have_committed(
    tmp_path, later_status
):
    db = tmp_path / "ledger.duckdb"
    loader._ensure_ledger(db)
    _seed(
        db,
        run_id="101",
        status=later_status,
        timestamp="2026-11-07T15:01:00+00:00",
    )

    with pytest.raises(
        RuntimeError, match="newer snapshot has a non-pending source status"
    ):
        loader._assert_replay_order(
            db,
            snapshot_id="100:1",
            order=loader.snapshot_order("2026-11-07T15:00:00Z", "100", "1"),
        )


def test_duckdb_ledger_migrates_existing_schema_without_updated_at(tmp_path):
    db = tmp_path / "legacy-ledger.duckdb"
    with duckdb.connect(str(db)) as connection:
        connection.execute("CREATE SCHEMA raw")
        connection.execute(
            """CREATE TABLE raw.toronto_snapshot_ledger (
                snapshot_id VARCHAR NOT NULL,
                source_run_id VARCHAR NOT NULL,
                source_run_attempt INTEGER NOT NULL,
                extraction_started_at_utc TIMESTAMPTZ NOT NULL,
                source_name VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                source_commit VARCHAR NOT NULL,
                error VARCHAR,
                PRIMARY KEY (snapshot_id, source_name)
            )"""
        )

    loader._ensure_ledger(db)
    _seed(db, run_id="100", status="failed")

    with duckdb.connect(str(db), read_only=True) as connection:
        columns = {
            row[0]
            for row in connection.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='raw' AND table_name='toronto_snapshot_ledger'"
            ).fetchall()
        }
        updated_at = connection.execute(
            "SELECT updated_at FROM raw.toronto_snapshot_ledger"
        ).fetchone()[0]

    assert "updated_at" in columns
    assert updated_at is not None


def test_archive_validates_both_sources_before_loading_either(tmp_path, monkeypatch):
    database = tmp_path / "preflight.duckdb"
    loader._ensure_ledger(database)
    extracted_at = "2026-11-07T15:00:00+00:00"
    columns = ["PERMIT_NUM"]
    members = {}
    summaries = {}

    for name in ("active_permits", "cleared_permits"):
        if name == "active_permits":
            import io

            buffer = io.BytesIO()
            pq.write_table(pa.table({"PERMIT_NUM": ["P-1"]}), buffer)
            payload = buffer.getvalue()
        else:
            # A ZIP-valid member whose Parquet payload is corrupt.
            payload = b"not a parquet file"
        digest = hashlib.sha256(payload).hexdigest()
        manifest = {
            "source_name": name,
            "status": "success",
            "warehouse_status": "not_loaded",
            "warehouse_eligible": True,
            "warehouse_quality_blockers": [],
            "validation_errors": [],
            "sha256": digest,
            "row_count": 1,
            "columns": columns,
            "required_columns": columns,
            "extracted_at_utc": extracted_at,
            "warehouse_since": None,
            "warehouse_date_column": None,
        }
        members[f"{name}/data.parquet"] = payload
        members[f"{name}/manifest.json"] = json.dumps(manifest).encode()
        summaries[name] = {
            "snapshot": f"{name}/data.parquet",
            "manifest": f"{name}/manifest.json",
            "sha256": digest,
            "row_count": 1,
            "columns": columns,
            "required_columns": columns,
            "extracted_at_utc": extracted_at,
            "warehouse_since": None,
            "warehouse_date_column": None,
            "warehouse_eligible": True,
            "warehouse_quality_blockers": [],
            "validation_errors": [],
        }

    bundle = {
        "format_version": 1,
        "snapshot_stage": "raw",
        "complete": True,
        "approved_for_warehouse": True,
        "source_run_id": "100",
        "source_run_attempt": "1",
        "source_commit": "a" * 40,
        "extraction_started_at_utc": extracted_at,
        "sources": summaries,
    }
    archive_path = tmp_path / "partial-corrupt-source.zip"
    with ZipFile(archive_path, "w", compression=ZIP_STORED) as archive:
        archive.writestr("bundle.json", json.dumps(bundle))
        for member, payload in members.items():
            archive.writestr(member, payload)

    sources = [
        SimpleNamespace(
            name=name,
            warehouse_load=True,
            warehouse_strategy="scd2",
            warehouse_table=f"raw_{name}",
            required_columns=("PERMIT_NUM",),
            warehouse_since=None,
            warehouse_date_column=None,
        )
        for name in ("active_permits", "cleared_permits")
    ]
    app = SimpleNamespace(
        sources=sources, warehouse_credentials=str(database), dataset_name="raw"
    )
    load_calls = []
    monkeypatch.setattr(
        loader, "load_scd2", lambda **kwargs: load_calls.append(kwargs["source"].name)
    )

    with pytest.raises(pa.ArrowInvalid, match="Parquet magic bytes"):
        loader._load_archive(
            archive_path,
            app=app,
            manifest_root=tmp_path / "manifests",
            release_tag="toronto-raw-snapshot-archive-2026-11",
            asset_name="toronto-raw-snapshot-bundle-100-1.zip",
        )

    assert load_calls == []


def test_historical_baseline_scope_selects_exact_runs_in_order():
    archives = [
        SimpleNamespace(run_id="103", order=(3, 103, 1)),
        SimpleNamespace(run_id="100", order=(1, 100, 1)),
        SimpleNamespace(run_id="102", order=(2, 102, 1)),
        SimpleNamespace(run_id="999", order=(4, 999, 1)),
    ]

    selected = loader._select_expected_run_archives(archives, ("100", "102", "103"))

    assert [archive.run_id for archive in selected] == ["100", "102", "103"]


def test_historical_baseline_scope_rejects_missing_run():
    archives = [SimpleNamespace(run_id="100", order=(1, 100, 1))]

    with pytest.raises(
        RuntimeError, match=r"missing approved legacy runs: \[.*101.*\]"
    ):
        loader._select_expected_run_archives(archives, ("100", "101"))


def test_historical_baseline_scope_rejects_multiple_attempts_for_one_run():
    archives = [
        SimpleNamespace(run_id="100", order=(1, 100, 1)),
        SimpleNamespace(run_id="100", order=(2, 100, 2)),
    ]

    with pytest.raises(ValueError, match="multiple archive attempts"):
        loader._select_expected_run_archives(archives, ("100",))

def test_historical_baseline_rejects_unexpected_archive_attempt():
    archives = [SimpleNamespace(run_id="100", attempt="2", order=(1, 100, 2))]

    with pytest.raises(ValueError, match="run 100 has archive attempt 2; expected attempt 1"):
        loader._select_expected_run_archives(archives, ("100",))


def test_historical_baseline_accepts_explicit_approved_attempt():
    archive = SimpleNamespace(run_id="100", attempt="2", order=(1, 100, 2))

    selected = loader._select_expected_run_archives(
        [archive], ("100",), expected_attempts={"100": "2"}
    )

    assert selected == [archive]


def test_persist_manifest_metadata_upsert_succeeds_with_duckdb_timestamp_function(
    tmp_path,
):
    database = tmp_path / "manifest.duckdb"
    loader._ensure_ledger(database)
    manifest_root = tmp_path / "manifests"
    first_manifest = {
        "source_name": "active_permits",
        "source_run_id": "100",
        "source_run_attempt": "1",
        "extracted_at_utc": "2026-11-07T15:00:00+00:00",
        "sha256": "a" * 64,
        "row_count": 1,
    }
    second_manifest = {
        **first_manifest,
        "source_run_id": "101",
        "extracted_at_utc": "2026-11-07T15:01:00+00:00",
        "sha256": "b" * 64,
    }

    # First call exercises INSERT; the second uses the same source_name and
    # must execute the ON CONFLICT DO UPDATE branch that previously failed.
    loader._persist_manifest(
        database,
        manifest_root,
        source_name="active_permits",
        manifest=first_manifest,
        release_tag="toronto-raw-snapshot-archive-2026-11",
        asset_name="toronto-raw-snapshot-bundle-100-1.zip",
    )
    loader._persist_manifest(
        database,
        manifest_root,
        source_name="active_permits",
        manifest=second_manifest,
        release_tag="toronto-raw-snapshot-archive-2026-12",
        asset_name="toronto-raw-snapshot-bundle-101-1.zip",
    )

    with duckdb.connect(str(database), read_only=True) as connection:
        row = connection.execute(
            "SELECT extracted_at_utc, manifest_json, release_tag, asset_name, updated_at "
            "FROM raw.toronto_source_snapshot_metadata WHERE source_name = ?",
            ["active_permits"],
        ).fetchone()

    assert row is not None
    assert row[0].astimezone(UTC).isoformat().startswith("2026-11-07T15:01:00")
    assert '"source_run_id": "101"' in row[1]
    assert row[2] == "toronto-raw-snapshot-archive-2026-12"
    assert row[3] == "toronto-raw-snapshot-bundle-101-1.zip"
    assert row[4] is not None


def test_duckdb_metrics_detect_scd2_integrity_violations(tmp_path):
    database = tmp_path / "metrics.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE SCHEMA raw")
        connection.execute(
            """CREATE TABLE raw.raw_active_permits (
                permit_num VARCHAR,
                revision_num VARCHAR,
                permit_type VARCHAR,
                _dlt_valid_from TIMESTAMP,
                _dlt_valid_to TIMESTAMP
            )"""
        )
        connection.execute(
            """INSERT INTO raw.raw_active_permits VALUES
                ('P-1', '00', 'Building', TIMESTAMP '2026-01-01', TIMESTAMP '2026-01-10'),
                ('P-1', '00', 'Building', TIMESTAMP '2026-01-02', TIMESTAMP '2026-01-03'),
                ('P-1', '00', 'Building', TIMESTAMP '2026-01-04', TIMESTAMP '2026-01-05'),
                ('P-1', '00', 'Building', TIMESTAMP '2026-01-10', NULL),
                ('P-2', '00', 'Plumbing', TIMESTAMP '2026-01-01', NULL),
                ('P-2', '00', 'Plumbing', TIMESTAMP '2026-01-02', NULL),
                ('P-3', '00', 'Electrical', NULL, NULL),
                ('P-4', '00', 'Demolition', TIMESTAMP '2026-01-05', TIMESTAMP '2026-01-05')"""
        )

        metrics = duckdb_validation._metrics(
            connection, "raw", "raw_active_permits"
        )

    assert metrics == {
        "total": 8,
        "current": 4,
        "expired": 4,
        "null_valid_from": 1,
        "invalid_intervals": 1,
        "duplicate_current_business_keys": 1,
        # Two nested P-1 overlaps plus one P-2 open-ended overlap. The
        # contiguous P-1 transition at Jan 10 is correctly not an overlap.
        "overlapping_intervals": 3,
    }


def test_duckdb_validator_rejects_unexpected_historical_row_counts():
    metrics = {
        "total": 1,
        "current": 1,
        "expired": 0,
        "null_valid_from": 0,
        "invalid_intervals": 0,
        "duplicate_current_business_keys": 0,
        "overlapping_intervals": 0,
    }

    with pytest.raises(ValueError, match="approved historical baseline"):
        duckdb_validation._validate_source_metrics("active_permits", metrics)


def test_provenance_validator_rejects_a_shared_but_unapproved_snapshot_set():
    snapshot_ids = [
        f"{run_id}:{duckdb_validation.EXPECTED_LEGACY_RUN_ATTEMPTS.get(run_id, '1')}"
        for run_id in duckdb_validation.EXPECTED_LEGACY_RUN_IDS
    ]
    run_ids = list(duckdb_validation.EXPECTED_LEGACY_RUN_IDS)
    snapshot_ids[-1] = "99999999999:1"
    run_ids[-1] = "99999999999"
    summary = {
        "snapshot_count": 28,
        "snapshot_ids": snapshot_ids,
        "snapshot_run_ids": run_ids,
    }

    with pytest.raises(ValueError, match="approved 28-run baseline"):
        provenance_validation._validate_snapshot_scope(summary, "DuckDB")
