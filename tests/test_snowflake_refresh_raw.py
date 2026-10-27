"""Offline safety tests for the canonical Snowflake RAW refresh script."""

from __future__ import annotations

import hashlib
import io
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from zipfile import ZIP_STORED, ZipFile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# The regular test environment installs the DuckDB extra, not the optional
# Snowflake connector. Provide a hard-failing stub if the connector is absent;
# these tests must never connect to a live account.
if "snowflake.connector" not in sys.modules:
    try:
        import snowflake.connector  # noqa: F401
    except ModuleNotFoundError:
        snowflake_package = ModuleType("snowflake")
        snowflake_package.__path__ = []
        connector_module = ModuleType("snowflake.connector")

        def _no_live_connection(**kwargs):
            raise AssertionError("Offline tests must not open Snowflake connections")

        connector_module.connect = _no_live_connection
        snowflake_package.connector = connector_module
        sys.modules["snowflake"] = snowflake_package
        sys.modules["snowflake.connector"] = connector_module

from scripts import snowflake_refresh_raw as refresh  # noqa: E402

RUN_ID = "123456789"
COMMIT = "a" * 40
SOURCE_ROWS = {
    "active_permits": {
        "PERMIT_NUM": "21 000001 BLD",
        "REVISION_NUM": "00",
        "PERMIT_TYPE": "New Houses",
        "APPLICATION_DATE": "2021-05-14",
        "ISSUED_DATE": "2021-06-14",
        "STATUS": "Open",
        "GEO_ID": "00123456",
    },
    "cleared_permits": {
        "PERMIT_NUM": "21 000002 BLD",
        "REVISION_NUM": "00",
        "PERMIT_TYPE": "New Houses",
        "APPLICATION_DATE": "2021-05-15",
        "ISSUED_DATE": "2021-06-15",
        "COMPLETED_DATE": "2021-07-15",
    },
}
REQUIRED_COLUMNS = {name: tuple(row) for name, row in SOURCE_ROWS.items()}
EXPECTED_MEMBERS = {
    "bundle.json",
    "active_permits/data.parquet",
    "active_permits/manifest.json",
    "cleared_permits/data.parquet",
    "cleared_permits/manifest.json",
}


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sources() -> dict:
    return {
        name: SimpleNamespace(
            name=name,
            required_columns=REQUIRED_COLUMNS[name],
            warehouse_since="2020-01-01",
            warehouse_date_column="APPLICATION_DATE",
        )
        for name in SOURCE_ROWS
    }


def _write_bundle(
    path: Path,
    *,
    extraction_started_at: datetime | None = None,
    omit_columns: dict[str, set[str]] | None = None,
) -> Path:
    started = extraction_started_at or (
        datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=2)
    )
    extracted = started + timedelta(minutes=1)
    omit_columns = omit_columns or {}
    members: dict[str, bytes] = {}
    summaries: dict[str, dict] = {}

    for name, source_row in SOURCE_ROWS.items():
        row = {
            key: value
            for key, value in source_row.items()
            if key not in omit_columns.get(name, set())
        }
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pylist([row]), buffer)
        parquet_bytes = buffer.getvalue()
        table = pq.read_table(io.BytesIO(parquet_bytes))
        columns = list(table.schema.names)
        digest = _sha256(parquet_bytes)
        manifest = {
            "source_name": name,
            "status": "success",
            "warehouse_status": "loaded",
            "sha256": digest,
            "row_count": 1,
            "columns": columns,
            "warehouse_since": "2020-01-01",
            "warehouse_date_column": "APPLICATION_DATE",
            "extracted_at_utc": extracted.isoformat(),
        }
        members[f"{name}/data.parquet"] = parquet_bytes
        members[f"{name}/manifest.json"] = json.dumps(manifest).encode()
        summaries[name] = {
            "sha256": digest,
            "row_count": 1,
            "columns": columns,
        }

    bundle = {
        "format_version": 1,
        "source_run_id": RUN_ID,
        "source_commit": COMMIT,
        "extraction_started_at_utc": started.isoformat(),
        "sources": summaries,
    }
    members["bundle.json"] = json.dumps(bundle).encode()
    assert set(members) == EXPECTED_MEMBERS
    with ZipFile(path, "w", compression=ZIP_STORED) as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return path


def _validate(bundle: Path, tmp_path: Path, *, run_id: str = RUN_ID) -> dict:
    return refresh._validate_and_extract_bundle(
        bundle,
        tmp_path / "extracted",
        _sources(),
        run_id,
        COMMIT,
    )


def _rewrite_bundle(
    original: Path, destination: Path, *, replacements: dict[str, bytes]
) -> Path:
    with (
        ZipFile(original) as source,
        ZipFile(destination, "w", compression=ZIP_STORED) as target,
    ):
        for name in source.namelist():
            target.writestr(name, replacements.get(name, source.read(name)))
    return destination


def test_valid_recent_bundle_is_validated_and_extracted(tmp_path: Path) -> None:
    bundle_path = _write_bundle(tmp_path / "bundle.zip")

    bundle = _validate(bundle_path, tmp_path)

    assert bundle["source_run_id"] == RUN_ID
    for name in SOURCE_ROWS:
        extracted = tmp_path / "extracted" / name / "data.parquet"
        assert extracted.is_file()
        assert pq.ParquetFile(extracted).metadata.num_rows == 1


def test_validator_rejects_wrong_selected_run_id(tmp_path: Path) -> None:
    bundle_path = _write_bundle(tmp_path / "bundle.zip")

    with pytest.raises(ValueError, match="run ID"):
        _validate(bundle_path, tmp_path, run_id="999999999")


def test_validator_rejects_stale_snapshot(tmp_path: Path) -> None:
    started = datetime.now(UTC) - timedelta(hours=37)
    bundle_path = _write_bundle(tmp_path / "stale.zip", extraction_started_at=started)

    with pytest.raises(ValueError, match="older than 36 hours"):
        _validate(bundle_path, tmp_path)


def test_validator_rejects_snapshot_checksum_mismatch(tmp_path: Path) -> None:
    bundle_path = _write_bundle(tmp_path / "bundle.zip")
    bad_bundle = _rewrite_bundle(
        bundle_path,
        tmp_path / "bad-checksum.zip",
        replacements={"active_permits/data.parquet": b"tampered parquet"},
    )

    with pytest.raises(ValueError, match="SHA-256"):
        _validate(bad_bundle, tmp_path)


def test_validator_rejects_missing_required_column(tmp_path: Path) -> None:
    bundle_path = _write_bundle(
        tmp_path / "missing-column.zip",
        omit_columns={"active_permits": {"STATUS"}},
    )

    with pytest.raises(ValueError, match="missing required columns"):
        _validate(bundle_path, tmp_path)


class _FakeCursor:
    def __init__(self, context: tuple[str, str, str]) -> None:
        self.context = context
        self.queries: list[str] = []
        self.closed = False

    def execute(self, query: str, params: object = None) -> None:
        self.queries.append(query)

    def fetchone(self) -> tuple[str, str, str]:
        return self.context

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self.fake_cursor = cursor
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self.fake_cursor

    def close(self) -> None:
        self.closed = True


def test_preflight_wrong_role_stops_before_table_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(("ACCOUNTADMIN", "TORONTO", "RAW"))
    connection = _FakeConnection(cursor)
    monkeypatch.setattr(
        refresh.snowflake.connector, "connect", lambda **kwargs: connection
    )

    with pytest.raises(ValueError, match="Unexpected Snowflake role"):
        refresh._connect_and_preflight({})

    assert len(cursor.queries) == 1
    assert cursor.closed
    assert connection.closed


def test_failed_preflight_prevents_both_table_loads(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sources = tuple(
        SimpleNamespace(
            name=name,
            warehouse_load=True,
            warehouse_strategy="scd2",
            warehouse_table=refresh.EXPECTED_TABLES[name],
        )
        for name in refresh.SOURCE_NAMES
    )
    monkeypatch.setattr(
        refresh, "load_config", lambda path: SimpleNamespace(sources=sources)
    )
    monkeypatch.setattr(refresh, "_validate_and_extract_bundle", lambda *args: {})
    monkeypatch.setattr(
        refresh,
        "_snowflake_connection_options",
        lambda credentials, key_path: {},
    )
    monkeypatch.setattr(
        refresh,
        "_connect_and_preflight",
        lambda options: (_ for _ in ()).throw(RuntimeError("preflight failed")),
    )
    load_calls: list[str] = []
    monkeypatch.setattr(
        refresh,
        "load_scd2",
        lambda **kwargs: load_calls.append(kwargs["source"].name),
    )
    bundle = tmp_path / "bundle.zip"
    bundle.write_bytes(b"synthetic placeholder; validator is mocked")
    monkeypatch.setenv(
        "TORONTO_WAREHOUSE_CREDENTIALS",
        "snowflake://user@account/?private_key=ZmFrZQ==",
    )
    monkeypatch.setenv("TORONTO_WAREHOUSE_KIND", "snowflake")
    monkeypatch.setenv("TORONTO_WAREHOUSE_DATASET", "RAW")
    monkeypatch.setenv("SNAPSHOT_SOURCE_RUN_ID", RUN_ID)
    monkeypatch.setenv("SNAPSHOT_SOURCE_COMMIT", COMMIT)
    monkeypatch.setenv("SNAPSHOT_BUNDLE_PATH", str(bundle))

    with pytest.raises(RuntimeError, match="preflight failed"):
        refresh.main()

    assert load_calls == []
