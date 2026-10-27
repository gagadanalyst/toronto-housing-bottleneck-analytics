from datetime import datetime

from toronto_housing_ingestion.dlt_loader import _record_in_warehouse_window, _row_hash


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
