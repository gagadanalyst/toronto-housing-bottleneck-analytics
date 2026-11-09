from datetime import datetime
from types import SimpleNamespace

import pyarrow as pa
from pyarrow import parquet as pq

from toronto_housing_ingestion.dlt_loader import (
    _analyze_scd2_snapshot,
    _destination_scope_excluding_keys,
    _destination_scope_matching_snapshot_keys,
    _iter_scd2_snapshot_rows,
    _record_in_warehouse_window,
    _resolve_scd2_key_columns,
    _row_hash,
    _validate_scd2_snapshot,
    load_scd2,
)


def test_destination_scope_preserves_resolved_column_names() -> None:
    scope = _destination_scope_excluding_keys(
        ("PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"),
        {("26 184407 BLD", "00", "New Houses")},
    )

    assert scope is not None
    assert '"PERMIT_NUM"' in scope
    assert '"REVISION_NUM"' in scope
    assert '"PERMIT_TYPE"' in scope
    assert '"permit_num"' not in scope


def test_destination_scope_matches_staged_keys_for_invalid_key_snapshot() -> None:
    scope = _destination_scope_matching_snapshot_keys(
        ("PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"),
        {("26 184407 BLD", "00", "New Houses")},
    )

    assert "EXISTS (SELECT 1 FROM {staging_table} AS source" in scope
    assert 'source."PERMIT_NUM" = {table}."PERMIT_NUM"' in scope
    assert 'source."REVISION_NUM" = {table}."REVISION_NUM"' in scope
    assert 'source."PERMIT_TYPE" = {table}."PERMIT_TYPE"' in scope
    assert "NOT ((" in scope


def test_warehouse_date_filter_keeps_2020_and_later() -> None:
    assert _record_in_warehouse_window(
        {"APPLICATION_DATE": datetime(2020, 1, 1)}, "APPLICATION_DATE", "2020-01-01"
    )
    assert _record_in_warehouse_window(
        {"APPLICATION_DATE": "2024-05-01"}, "APPLICATION_DATE", "2020-01-01"
    )
    assert not _record_in_warehouse_window(
        {"APPLICATION_DATE": "2019-12-31"}, "APPLICATION_DATE", "2020-01-01"
    )
    assert not _record_in_warehouse_window(
        {"APPLICATION_DATE": None}, "APPLICATION_DATE", "2020-01-01"
    )


def test_row_hash_normalizes_numeric_flags_to_legacy_float_form() -> None:
    fields = (
        "ASSEMBLY",
        "BUSINESS_AND_PERSONAL_SERVICES",
        "DEMOLITION",
        "INDUSTRIAL",
        "INSTITUTIONAL",
        "INTERIOR_ALTERATIONS",
        "MERCANTILE",
        "RESIDENTIAL",
    )
    float_row = {field: 0.0 for field in fields}
    int_row = {field: 0 for field in fields}

    assert _row_hash(float_row) == _row_hash(int_row)


def _test_source(**overrides):
    values = {
        "name": "active_permits",
        "primary_key": ("permit_num", "revision_num", "permit_type"),
        "warehouse_date_column": None,
        "warehouse_since": None,
        "warehouse_load": True,
        "warehouse_strategy": "scd2",
        "warehouse_table": "raw_active_permits",
        "type_hints": {},
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _write_duplicate_snapshot(path, second_builder):
    rows = [
        {
            "_id": 10664,
            "PERMIT_NUM": "26 184407 BLD",
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New Houses",
            "BUILDER_NAME": "STANLEY MP INC",
        },
        {
            "_id": 11167,
            "PERMIT_NUM": "26 184407 BLD",
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New Houses",
            "BUILDER_NAME": second_builder,
        },
    ]
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_scd2_prevalidation_allows_and_deduplicates_identical_tracked_rows(
    tmp_path,
) -> None:
    snapshot = tmp_path / "duplicate.parquet"
    _write_duplicate_snapshot(snapshot, "STANLEY MP INC")
    source = _test_source()
    key_columns = _resolve_scd2_key_columns(snapshot, source)

    conflicts, quarantined_row_count = _validate_scd2_snapshot(
        snapshot,
        source=source,
        key_columns=key_columns,
        preserve_row_hash=False,
    )
    assert conflicts == []
    assert quarantined_row_count == 0

    loaded_rows = list(
        _iter_scd2_snapshot_rows(
            snapshot,
            key_columns=key_columns,
            date_column=None,
            since=None,
            preserve_row_hash=False,
            source_name=source.name,
            quarantined_keys=set(),
        )
    )
    assert len(loaded_rows) == 1
    assert loaded_rows[0]["_id"] == 10664


def test_scd2_quarantines_conflict_and_preserves_history(tmp_path) -> None:
    import duckdb

    source = _test_source()
    database = tmp_path / "quarantine.duckdb"
    original_snapshot = tmp_path / "original.parquet"
    next_snapshot = tmp_path / "next.parquet"
    pipeline_name = f"test_scd2_quarantine_{tmp_path.name}"

    def permit(source_id, permit_num, builder):
        return {
            "_id": source_id,
            "PERMIT_NUM": permit_num,
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New Houses",
            "BUILDER_NAME": builder,
        }

    pq.write_table(
        pa.Table.from_pylist(
            [
                permit(10395, "26 184407 BLD", "STANLEY MP INC"),
                permit(20001, "26 100001 BLD", "OLD BUILDER"),
                permit(20002, "26 100002 BLD", "REMOVED BUILDER"),
            ]
        ),
        original_snapshot,
    )

    load_scd2(
        credentials=str(database),
        snapshot_path=original_snapshot,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
        pipeline_name=pipeline_name,
    )

    pq.write_table(
        pa.Table.from_pylist(
            [
                permit(10664, "26 184407 BLD", "STANLEY MP INC"),
                permit(11167, "26 184407 BLD", "AGARWOOD CONSTRUCTION INC"),
                permit(20001, "26 100001 BLD", "NEW BUILDER"),
                permit(20003, "26 100003 BLD", "INSERTED BUILDER"),
            ]
        ),
        next_snapshot,
    )

    result = load_scd2(
        credentials=str(database),
        snapshot_path=next_snapshot,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
        pipeline_name=pipeline_name,
    )

    assert len(result.quarantined_business_keys) == 1
    assert result.quarantined_row_count == 2
    assert result.quarantined_business_keys[0]["business_key"] == {
        "permit_num": "26 184407 BLD",
        "revision_num": "00",
        "permit_type": "New Houses",
    }

    with duckdb.connect(str(database), read_only=True) as connection:

        def history(permit_num):
            return connection.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (WHERE _dlt_valid_to IS NULL),
                    MAX(CASE
                        WHEN _dlt_valid_to IS NULL THEN builder_name
                    END)
                FROM raw.raw_active_permits
                WHERE permit_num = ?
                  AND revision_num = '00'
                  AND permit_type = 'New Houses'
                """,
                [permit_num],
            ).fetchone()

        # Both conflicting source records were excluded. October remains current.
        assert history("26 184407 BLD") == (1, 1, "STANLEY MP INC")

        # A legitimate builder change still creates a new SCD2 version.
        assert history("26 100001 BLD") == (2, 1, "NEW BUILDER")

        # A genuinely missing permit is still retired.
        assert history("26 100002 BLD") == (1, 0, None)

        # A new permit is still inserted.
        assert history("26 100003 BLD") == (1, 1, "INSERTED BUILDER")


def test_quarantine_preserves_invalid_conflicting_and_identical_rows(tmp_path) -> None:
    import json

    import duckdb

    source = _test_source()
    snapshot = tmp_path / "mixed-quality.parquet"
    baseline_snapshot = tmp_path / "baseline.parquet"
    clean_snapshot = tmp_path / "clean-after-quarantine.parquet"
    database = tmp_path / "mixed-quality.duckdb"
    pipeline_name = f"test_scd2_mixed_quarantine_{tmp_path.name}"

    def permit(source_id, permit_num, builder):
        return {
            "_id": source_id,
            "PERMIT_NUM": permit_num,
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New Houses",
            "BUILDER_NAME": builder,
        }

    pq.write_table(
        pa.Table.from_pylist(
            [
                permit(10, "26 100001 BLD", "SAME BUILDER"),
                permit(11, "26 100002 BLD", "OLD CONFLICT BUILDER"),
                permit(12, "26 100003 BLD", "POTENTIALLY MISSING BUILDER"),
                permit(13, "26 100004 BLD", "ALSO POTENTIALLY MISSING"),
            ]
        ),
        baseline_snapshot,
    )
    load_scd2(
        credentials=str(database),
        snapshot_path=baseline_snapshot,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
        pipeline_name=pipeline_name,
    )

    rows = [
        {
            "_id": 1,
            "PERMIT_NUM": "26 100001 BLD",
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New Houses",
            "BUILDER_NAME": "SAME BUILDER",
        },
        {
            "_id": 2,
            "PERMIT_NUM": "26 100001 BLD",
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New Houses",
            "BUILDER_NAME": "SAME BUILDER",
        },
        {
            "_id": 3,
            "PERMIT_NUM": "26 100002 BLD",
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New Houses",
            "BUILDER_NAME": "OLD BUILDER",
        },
        {
            "_id": 4,
            "PERMIT_NUM": "26 100002 BLD",
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New Houses",
            "BUILDER_NAME": "NEW BUILDER",
        },
        {
            "_id": 5,
            "PERMIT_NUM": None,
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New Houses",
            "BUILDER_NAME": "INVALID NULL KEY",
        },
        {
            "_id": 6,
            "PERMIT_NUM": "   ",
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New Houses",
            "BUILDER_NAME": "INVALID BLANK KEY",
        },
    ]
    pq.write_table(pa.Table.from_pylist(rows), snapshot)
    key_columns = _resolve_scd2_key_columns(snapshot, source)
    analysis = _analyze_scd2_snapshot(
        snapshot, source=source, key_columns=key_columns, preserve_row_hash=False
    )

    assert analysis.bad_row_count == 4
    assert analysis.business_key_count == 1
    assert analysis.duplicate_row_count == 1
    assert analysis.invalid_key_row_count == 2
    assert len(analysis.quarantine_rows) == 5
    assert {item["reason"] for item in analysis.quarantine_rows} == {
        "identical_duplicate_record",
        "conflicting_duplicate_business_key",
        "invalid_business_key",
    }

    result = load_scd2(
        credentials=str(database),
        snapshot_path=snapshot,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
        pipeline_name=pipeline_name,
        snapshot_context={
            "snapshot_id": "12345:1",
            "source_run_id": "12345",
            "source_run_attempt": 1,
            "extracted_at_utc": "2026-11-09T01:00:00+00:00",
            "source_commit": "a" * 40,
            "release_tag": "toronto-raw-snapshot-archive-2026-11",
            "asset_name": "toronto-raw-snapshot-bundle-12345-1.zip",
            "parquet_sha256": "b" * 64,
        },
    )
    assert result.quarantined_row_count == 5
    assert result.quarantined_bad_row_count == 4
    assert result.quarantined_business_key_count == 1
    assert result.quarantine_sha256

    with duckdb.connect(str(database), read_only=True) as connection:
        stored = connection.execute(
            "SELECT row_ordinal, reason, raw_record_json, snapshot_id, "
            "source_commit, release_tag, asset_name, parquet_sha256 "
            "FROM raw.toronto_scd2_quarantine ORDER BY row_ordinal"
        ).fetchall()
        clean = connection.execute(
            "SELECT permit_num, builder_name FROM raw.raw_active_permits "
            "WHERE _dlt_valid_to IS NULL ORDER BY permit_num"
        ).fetchall()

    assert len(stored) == 5
    assert all(row[2] for row in stored)
    assert all(row[3] == "12345:1" and row[4] == "a" * 40 for row in stored)
    assert all(row[5].endswith("2026-11") for row in stored)
    assert all(row[7] == "b" * 64 for row in stored)
    assert any(
        json.loads(row[2]).get("BUILDER_NAME") == "INVALID NULL KEY" for row in stored
    )
    assert any(
        json.loads(row[2]).get("BUILDER_NAME") == "INVALID BLANK KEY" for row in stored
    )
    # The invalid source keys make the snapshot incomplete. Do not retire any
    # destination key that is absent from the clean staged rows; the malformed
    # source row cannot identify which prior permit it represents.
    assert clean == [
        ("26 100001 BLD", "SAME BUILDER"),
        ("26 100002 BLD", "OLD CONFLICT BUILDER"),
        ("26 100003 BLD", "POTENTIALLY MISSING BUILDER"),
        ("26 100004 BLD", "ALSO POTENTIALLY MISSING"),
    ]

    # A later clean full snapshot restores normal SCD2 retirement semantics.
    pq.write_table(
        pa.Table.from_pylist(
            [
                permit(20, "26 100001 BLD", "SAME BUILDER"),
                permit(21, "26 100005 BLD", "NEW CLEAN PERMIT"),
            ]
        ),
        clean_snapshot,
    )
    load_scd2(
        credentials=str(database),
        snapshot_path=clean_snapshot,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
        pipeline_name=pipeline_name,
    )
    with duckdb.connect(str(database), read_only=True) as connection:
        current_after_clean = connection.execute(
            "SELECT permit_num, builder_name FROM raw.raw_active_permits "
            "WHERE _dlt_valid_to IS NULL ORDER BY permit_num"
        ).fetchall()
    assert current_after_clean == [
        ("26 100001 BLD", "SAME BUILDER"),
        ("26 100005 BLD", "NEW CLEAN PERMIT"),
    ]
