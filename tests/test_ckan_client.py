import pandas as pd
import pytest
import requests

from toronto_housing_ingestion import utils
from toronto_housing_ingestion.ckan_client import (
    DatastoreUnavailableError,
    TorontoCKANClient,
)


def client() -> TorontoCKANClient:
    return TorontoCKANClient("https://example.com/api", timeout_seconds=1, retries=0)


def test_selects_datastore_first() -> None:
    c = client()
    package = {
        "resources": [
            {"id": "csv", "format": "CSV", "url": "https://x/c.csv"},
            {"id": "ds", "datastore_active": True},
        ]
    }
    assert c.select_resource(package)["id"] == "ds"
    c.close()


def test_selects_csv_before_json_and_skips_datastore_dump() -> None:
    c = client()
    package = {
        "resources": [
            {
                "id": "datastore-dump",
                "datastore_active": True,
                "format": "CSV",
                "url": "https://x/datastore/dump/ds",
            },
            {"id": "json", "format": "JSON", "url": "https://x/j.json"},
            {"id": "csv", "format": "CSV", "url": "https://x/c.csv"},
        ]
    }
    assert [resource["id"] for resource in c.fallback_resources(package)] == [
        "csv",
        "json",
    ]
    c.close()


def test_probe_wraps_api_failure(monkeypatch) -> None:
    c = client()

    def fail(*args, **kwargs):
        raise requests.RequestException("down")

    monkeypatch.setattr(c, "action", fail)
    with pytest.raises(DatastoreUnavailableError):
        c.probe_datastore("1")
    c.close()


def test_json_frame_iterator_requires_ijson() -> None:
    pytest.importorskip("ijson")


def test_datastore_fallback_error_type_is_clear() -> None:
    error = DatastoreUnavailableError("DataStore down")
    assert "DataStore down" in str(error)


def test_probe_datastore_does_not_send_date_filter(monkeypatch) -> None:
    c = client()
    captured = {}

    def fake_action(name, **params):
        captured.update(params)
        return {
            "records": [{"id": 1}],
            "fields": [{"id": "id", "type": "int4"}],
            "total": 1,
        }

    monkeypatch.setattr(c, "action", fake_action)
    c.probe_datastore("resource")
    assert "filters" not in captured
    c.close()


def test_configures_retries_for_transient_http_errors() -> None:
    with TorontoCKANClient(
        "https://example.com/api",
        timeout_seconds=7,
        retries=3,
    ) as c:
        retry = c.session.adapters["https://"].max_retries

        assert c.timeout_seconds == 7
        assert retry.total == 3
        assert retry.connect == 3
        assert retry.read == 3
        assert retry.status == 3
        assert set(retry.status_forcelist) == {429, 500, 502, 503, 504}
        assert retry.respect_retry_after_header
        assert retry.allowed_methods == frozenset({"GET", "HEAD"})


def test_action_passes_configured_timeout(monkeypatch) -> None:
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"success": True, "result": {"id": "active"}}

    with TorontoCKANClient(
        "https://example.com/api",
        timeout_seconds=13,
        retries=0,
    ) as c:

        def fake_get(url, *, params, timeout):
            captured.update(url=url, params=params, timeout=timeout)
            return FakeResponse()

        monkeypatch.setattr(c.session, "get", fake_get)

        result = c.action("package_show", id="active-permits")

    assert result == {"id": "active"}
    assert captured["timeout"] == 13
    assert captured["params"] == {"id": "active-permits"}
    assert captured["url"].endswith("/package_show")


def test_fetch_datastore_paginates_with_sort_and_keeps_all_rows(
    monkeypatch, tmp_path
) -> None:
    pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    calls = []

    def fake_action(action_name, **params):
        assert action_name == "datastore_search"
        calls.append(params.copy())

        if params["offset"] == 0:
            return {
                "records": [{"id": 1}, {"id": 2}],
                "total": 3,
            }

        return {
            "records": [{"id": 3}],
            "total": 3,
        }

    output = tmp_path / "data.parquet"

    with client() as c:
        monkeypatch.setattr(c, "action", fake_action)

        count = c.fetch_datastore(
            resource_id="active-resource",
            page_size=2,
            checkpoint_dir=tmp_path / "checkpoints",
            fingerprint={"resource_hash": "test"},
            source_name="active_permits",
            output_path=output,
            pagination_sort="_id asc",
            required_columns=("id",),
            date_columns=(),
        )

    assert count == 3
    assert [call["offset"] for call in calls] == [0, 2]
    assert all(call["limit"] == 2 for call in calls)
    assert all(call["sort"] == "_id asc" for call in calls)
    assert all(call["resource_id"] == "active-resource" for call in calls)
    assert pq.read_table(output).column("id").to_pylist() == [1, 2, 3]


def test_fetch_datastore_restarts_from_zero_when_total_changes(
    monkeypatch, tmp_path
) -> None:
    pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    calls = []
    delays = []

    def fake_action(action_name, **params):
        calls.append(params.copy())
        if len(calls) == 1:
            return {"records": [{"id": 1}, {"id": 2}], "total": 4}
        if len(calls) == 2:
            return {"records": [{"id": 3}], "total": 3}
        if params["offset"] == 0:
            return {"records": [{"id": 1}, {"id": 2}], "total": 3}
        return {"records": [{"id": 3}], "total": 3}

    output = tmp_path / "data.parquet"
    checkpoint_dir = tmp_path / "checkpoints"

    with client() as c:
        monkeypatch.setattr(c, "action", fake_action)
        monkeypatch.setattr(
            "toronto_housing_ingestion.ckan_client.time.sleep", delays.append
        )
        count = c.fetch_datastore(
            resource_id="active-resource",
            page_size=2,
            checkpoint_dir=checkpoint_dir,
            fingerprint={"resource_hash": "test"},
            source_name="active_permits",
            output_path=output,
            pagination_sort="_id asc",
            required_columns=("id",),
            date_columns=(),
        )

    assert count == 3
    assert [call["offset"] for call in calls] == [0, 2, 0, 2]
    assert delays == [5]
    assert pq.read_table(output).column("id").to_pylist() == [1, 2, 3]
    assert not checkpoint_dir.exists()


def test_fetch_datastore_fails_cleanly_when_total_keeps_changing(
    monkeypatch, tmp_path
) -> None:
    pytest.importorskip("pyarrow")
    delays = []

    def fake_action(action_name, **params):
        if params["offset"] == 0:
            return {"records": [{"id": 1}, {"id": 2}], "total": 4}
        return {"records": [{"id": 3}], "total": 3}

    output = tmp_path / "data.parquet"
    checkpoint_dir = tmp_path / "checkpoints"

    with client() as c:
        monkeypatch.setattr(c, "action", fake_action)
        monkeypatch.setattr(
            "toronto_housing_ingestion.ckan_client.time.sleep", delays.append
        )
        with pytest.raises(RuntimeError, match="DataStore total kept changing"):
            c.fetch_datastore(
                resource_id="active-resource",
                page_size=2,
                checkpoint_dir=checkpoint_dir,
                fingerprint={"resource_hash": "unstable"},
                source_name="active_permits",
                output_path=output,
                pagination_sort="_id asc",
                required_columns=("id",),
                date_columns=(),
            )

    assert delays == [5, 10]
    assert not output.exists()
    assert not checkpoint_dir.exists()


@pytest.mark.parametrize(
    ("fmt", "payload"),
    [
        (
            "csv",
            b"id,APPLICATION_DATE\n1,2019-12-31\n2,2020-01-01\n",
        ),
        (
            "json",
            b'{"result":{"records":['
            b'{"id":1,"APPLICATION_DATE":"2019-12-31"},'
            b'{"id":2,"APPLICATION_DATE":"2020-01-01"}'
            b"]}}",
        ),
    ],
)
def test_download_fallback_materializes_csv_and_json_offline(
    fmt, payload, monkeypatch, tmp_path
) -> None:
    pytest.importorskip("pyarrow")
    if fmt == "json":
        pytest.importorskip("ijson")

    import pyarrow.parquet as pq

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            yield payload

    output = tmp_path / f"fallback.{fmt}.parquet"

    with client() as c:

        def fake_get(url, **kwargs):
            assert kwargs["stream"] is True
            assert kwargs["timeout"] == c.timeout_seconds
            return FakeResponse()

        monkeypatch.setattr(c.session, "get", fake_get)

        count = c.download_fallback(
            resource={
                "id": f"{fmt}-resource",
                "format": fmt,
                "url": f"https://example.com/data.{fmt}",
            },
            output_path=output,
            source_name="active_permits",
            required_columns=("id", "APPLICATION_DATE"),
            date_columns=("APPLICATION_DATE",),
        )

    assert output.is_file()
    assert count == 2
    assert pq.read_table(output).column("id").to_pylist() == [1, 2]


def test_csv_fallback_handles_mixed_revision_types_across_chunks(
    monkeypatch, tmp_path
) -> None:
    """Keep column types consistent when CSV chunks infer different types."""
    pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    csv_lines = [
        "PERMIT_NUM,REVISION_NUM,PERMIT_TYPE,APPLICATION_DATE,EST_CONST_COST,"
        "GEO_ID,DWELLING_UNITS_CREATED,DWELLING_UNITS_LOST"
    ]
    csv_lines.extend(
        f"P{i:05d},{i},BLD,2024-01-01,{i * 1000}.00,{i:05d},{i % 5},{(i + 1) % 5}"
        for i in range(1, 10_001)
    )
    csv_lines.append(
        "P10001,revision-X,BLD,2024-01-01,DO NOT UPDATE OR DELETE THIS INFO FIELD,"
        "geo-X,not stated,2"
    )
    payload = ("\n".join(csv_lines) + "\n").encode("utf-8")

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            yield payload

    output = tmp_path / "mixed-revisions.parquet"

    with client() as c:

        def fake_get(url, **kwargs):
            assert kwargs["stream"] is True
            return FakeResponse()

        monkeypatch.setattr(c.session, "get", fake_get)

        count = c.download_fallback(
            resource={
                "id": "cleared-csv",
                "format": "csv",
                "url": "https://example.com/cleared.csv",
            },
            output_path=output,
            source_name="cleared_permits",
            required_columns=(
                "PERMIT_NUM",
                "REVISION_NUM",
                "PERMIT_TYPE",
                "APPLICATION_DATE",
                "EST_CONST_COST",
            ),
            date_columns=("APPLICATION_DATE",),
        )

    assert count == 10_001
    table = pq.read_table(output)
    revisions = table.column("REVISION_NUM").to_pylist()
    costs = table.column("EST_CONST_COST").to_pylist()
    assert revisions[0] == "1"
    assert revisions[-1] == "revision-X"
    assert costs[-1] == "DO NOT UPDATE OR DELETE THIS INFO FIELD"
    for column in ("GEO_ID", "DWELLING_UNITS_CREATED", "DWELLING_UNITS_LOST"):
        assert str(table.schema.field(column).type) == "string"
    assert table.column("GEO_ID").to_pylist()[0] == "00001"
    assert table.column("GEO_ID").to_pylist()[-1] == "geo-X"
    assert table.column("DWELLING_UNITS_CREATED").to_pylist()[0] == "1"
    assert table.column("DWELLING_UNITS_CREATED").to_pylist()[-1] == "not stated"


@pytest.mark.parametrize("snapshot_exists", [False, True])
def test_completed_checkpoint_mismatch_restarts_extraction(
    monkeypatch, tmp_path, snapshot_exists
) -> None:
    pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    checkpoint_dir = tmp_path / "checkpoints"
    checkpoint_dir.mkdir()
    fingerprint = {"resource_hash": "stable"}

    utils.atomic_write_json(
        checkpoint_dir / "checkpoint.json",
        {
            "resource_id": "active-resource",
            "fingerprint": fingerprint,
            "next_offset": 3,
            "next_page": 1,
            "declared_total": 3,
            "complete": True,
        },
    )
    utils.atomic_write_parquet(
        pd.DataFrame({"id": [1, 2]}),
        checkpoint_dir / "part_000000.parquet",
    )

    output_path = tmp_path / "data.parquet"
    if snapshot_exists:
        utils.atomic_write_parquet(pd.DataFrame({"id": [1, 2]}), output_path)

    offsets = []

    def fake_action(action_name, **params):
        assert action_name == "datastore_search"
        offsets.append(params["offset"])
        if params["offset"] == 0:
            return {"records": [{"id": 1}, {"id": 2}], "total": 3}
        return {"records": [{"id": 3}], "total": 3}

    with client() as c:
        monkeypatch.setattr(c, "action", fake_action)
        count = c.fetch_datastore(
            resource_id="active-resource",
            page_size=2,
            checkpoint_dir=checkpoint_dir,
            fingerprint=fingerprint,
            source_name="active_permits",
            output_path=output_path,
            pagination_sort="_id asc",
            required_columns=("id",),
            date_columns=(),
        )

    assert count == 3
    assert offsets == [0, 2]
    assert pq.read_table(output_path).column("id").to_pylist() == [1, 2, 3]
    assert not checkpoint_dir.exists()


def _full_source_schema_test_records():
    first = {
        "_id": 101,
        "PERMIT_NUM": "24 172231 BLD",
        "REVISION_NUM": "00",
        "PERMIT_TYPE": "Building Additions/Alterations",
        "STRUCTURE_TYPE": "SFD - Detached",
        "WORK": "Addition",
        "STREET_NUM": "0012",
        "STREET_NAME": "MAIN",
        "STREET_TYPE": "ST",
        "STREET_DIRECTION": None,
        "POSTAL": "M5V",
        "GEO_ID": "352052",
        "WARD_GRID": "N0624",
        "APPLICATION_DATE": "2024-01-01",
        "ISSUED_DATE": "2024-01-02",
        "COMPLETED_DATE": None,
        "STATUS": "Closed",
        "DESCRIPTION": "Test description",
        "CURRENT_USE": "Single Family Dwelling",
        "PROPOSED_USE": "Single Family Dwelling",
        "DWELLING_UNITS_CREATED": "00",
        "DWELLING_UNITS_LOST": "0001",
        "EST_CONST_COST": "DO NOT UPDATE OR DELETE THIS INFO FIELD",
        "ASSEMBLY": 0,
        "INSTITUTIONAL": 0.0,
        "RESIDENTIAL": 55.5,
        "BUSINESS_AND_PERSONAL_SERVICES": 0,
        "MERCANTILE": 0.0,
        "INDUSTRIAL": 0,
        "INTERIOR_ALTERATIONS": 12.0,
        "DEMOLITION": 0,
        "BUILDER_NAME": None,
    }
    second = {
        "_id": 102,
        "PERMIT_NUM": "25 174642 BLD",
        "REVISION_NUM": "02",
        "PERMIT_TYPE": "Building Additions/Alterations",
        "STRUCTURE_TYPE": "SFD - Detached",
        "WORK": "Interior Alterations",
        "STREET_NUM": "42",
        "STREET_NAME": "QUEEN",
        "STREET_TYPE": "AVE",
        "STREET_DIRECTION": "E",
        "POSTAL": "M5H",
        "GEO_ID": "123",
        "WARD_GRID": "N0624",
        "APPLICATION_DATE": "2024-02-01",
        "ISSUED_DATE": "2024-02-02",
        "COMPLETED_DATE": "2024-02-03",
        "STATUS": "Closed",
        "DESCRIPTION": "Another test description",
        "CURRENT_USE": "Retail",
        "PROPOSED_USE": "Retail",
        "DWELLING_UNITS_CREATED": "1",
        "DWELLING_UNITS_LOST": "0",
        "EST_CONST_COST": "25000.00",
        "ASSEMBLY": 0.0,
        "INSTITUTIONAL": 0,
        "RESIDENTIAL": 0,
        "BUSINESS_AND_PERSONAL_SERVICES": 12.5,
        "MERCANTILE": 20,
        "INDUSTRIAL": 0,
        "INTERIOR_ALTERATIONS": 0.0,
        "DEMOLITION": 0,
        "BUILDER_NAME": "NA",
    }
    return [first, second]


def _assert_full_source_schema_table(table):
    import toronto_housing_ingestion.ckan_client as ckan_client_module

    assert table.schema.names == list(ckan_client_module._SOURCE_FIELD_TYPES)
    for column, declared_type in ckan_client_module._SOURCE_FIELD_TYPES.items():
        actual = str(table.schema.field(column).type)
        if declared_type in {"text", "date"}:
            assert actual == "string", (column, declared_type, actual)
        elif declared_type == "float8":
            assert actual == "double", (column, declared_type, actual)
        elif declared_type in {"int2", "int4", "int8"}:
            expected_integer_type = {"int2": "int16", "int4": "int32", "int8": "int64"}[
                declared_type
            ]
            assert actual == expected_integer_type, (column, declared_type, actual)


def test_source_type_map_covers_all_32_current_columns():
    import toronto_housing_ingestion.ckan_client as ckan_client_module

    assert len(ckan_client_module._SOURCE_FIELD_TYPES) == 32
    assert set(ckan_client_module._SOURCE_TEXT_COLUMNS) == {
        name
        for name, field_type in ckan_client_module._SOURCE_FIELD_TYPES.items()
        if field_type in {"text", "date"}
    }
    assert ckan_client_module._SOURCE_FIELD_TYPES["GEO_ID"] == "text"
    assert ckan_client_module._SOURCE_FIELD_TYPES["DWELLING_UNITS_CREATED"] == "text"
    assert ckan_client_module._SOURCE_FIELD_TYPES["DWELLING_UNITS_LOST"] == "text"
    assert ckan_client_module._SOURCE_FIELD_TYPES["STREET_NUM"] == "text"


def test_all_extraction_routes_normalize_complete_source_schema(
    monkeypatch, tmp_path
) -> None:
    pytest.importorskip("pyarrow")
    pytest.importorskip("ijson")
    import csv as csv_module
    import io
    import json as json_module

    import pyarrow.parquet as pq

    records = _full_source_schema_test_records()
    headers = list(records[0])

    # The independent JSON export must preserve metadata-declared text values.
    # A separate negative test ensures unexpected numeric text values fail loudly.
    json_records = [dict(record) for record in records]

    csv_buffer = io.StringIO(newline="")
    writer = csv_module.DictWriter(csv_buffer, fieldnames=headers, lineterminator="\n")
    writer.writeheader()
    for record in records:
        writer.writerow(
            {key: "" if value is None else value for key, value in record.items()}
        )
    csv_payload = csv_buffer.getvalue().encode("utf-8")
    json_payload = json_module.dumps({"result": {"records": json_records}}).encode(
        "utf-8"
    )

    class FakeResponse:
        def __init__(self, payload):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            yield self.payload

    date_columns = ("APPLICATION_DATE", "ISSUED_DATE", "COMPLETED_DATE")
    required_columns = ("PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE")
    output_tables = []

    datastore_output = tmp_path / "all-fields-datastore.parquet"
    with client() as c:
        monkeypatch.setattr(
            c,
            "action",
            lambda action_name, **params: {
                "records": records,
                "total": len(records),
            },
        )
        row_count = c.fetch_datastore(
            resource_id="test-resource",
            page_size=10,
            checkpoint_dir=tmp_path / "datastore-checkpoints",
            fingerprint={"test": "all-source-fields"},
            source_name="cleared_permits",
            output_path=datastore_output,
            pagination_sort="_id asc",
            required_columns=required_columns,
            date_columns=date_columns,
        )
    assert row_count == len(records)
    output_tables.append(pq.read_table(datastore_output))

    for fmt, payload in (("csv", csv_payload), ("json", json_payload)):
        output_path = tmp_path / f"all-fields-{fmt}.parquet"
        with client() as c:
            monkeypatch.setattr(
                c.session,
                "get",
                lambda url, payload=payload, **kwargs: FakeResponse(payload),
            )
            row_count = c.download_fallback(
                resource={
                    "id": f"{fmt}-resource",
                    "format": fmt,
                    "url": f"https://example.com/data.{fmt}",
                },
                output_path=output_path,
                source_name="cleared_permits",
                required_columns=required_columns,
                date_columns=date_columns,
            )
        assert row_count == len(records)
        table = pq.read_table(output_path)
        _assert_full_source_schema_table(table)
        output_tables.append(table)

    for table in output_tables:
        _assert_full_source_schema_table(table)
        rows = table.to_pylist()
        assert rows[0]["STREET_NUM"] == "0012"
        assert rows[0]["DWELLING_UNITS_CREATED"] == "00"
        assert rows[0]["DWELLING_UNITS_LOST"] == "0001"
        assert rows[0]["EST_CONST_COST"] == "DO NOT UPDATE OR DELETE THIS INFO FIELD"
        assert rows[0]["COMPLETED_DATE"] is None
        assert rows[0]["BUILDER_NAME"] is None
        assert rows[0]["GEO_ID"] == "352052"
        assert rows[1]["GEO_ID"] == "123"
        assert rows[1]["DWELLING_UNITS_CREATED"] == "1"
        assert rows[1]["DWELLING_UNITS_LOST"] == "0"
        assert rows[1]["BUILDER_NAME"] == "NA"

    assert output_tables[0].to_pylist() == output_tables[1].to_pylist()
    assert output_tables[0].to_pylist() == output_tables[2].to_pylist()


def test_source_text_field_rejects_numeric_json_values() -> None:
    import toronto_housing_ingestion.ckan_client as ckan_client_module

    frame = pd.DataFrame({"GEO_ID": [352052]})
    with pytest.raises(ValueError, match="GEO_ID.*non-string value"):
        ckan_client_module._normalize_source_schema(frame)
