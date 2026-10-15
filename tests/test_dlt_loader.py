from datetime import datetime

from toronto_housing_ingestion.dlt_loader import _record_in_warehouse_window


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
