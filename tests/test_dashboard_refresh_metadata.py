from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dashboard.refresh_metadata import format_refresh_timestamp


def test_formats_utc_refresh_timestamp_in_toronto_daylight_time() -> None:
    timestamp = datetime(2026, 10, 28, 20, 0, tzinfo=UTC)

    assert format_refresh_timestamp(timestamp) == "28 Oct 2026 · 04:00 PM"


def test_formats_winter_refresh_timestamp_in_eastern_standard_time() -> None:
    timestamp = datetime(2026, 1, 15, 20, 0, tzinfo=UTC)

    assert format_refresh_timestamp(timestamp) == "15 Jan 2026 · 03:00 PM"


def test_naive_timestamp_is_interpreted_as_utc() -> None:
    timestamp = datetime(2026, 1, 15, 12, 0)

    assert format_refresh_timestamp(timestamp) == "15 Jan 2026 · 07:00 AM"


@pytest.mark.parametrize("value", [None, "", "not-a-timestamp"])
def test_missing_or_invalid_timestamp_uses_fallback(value: object) -> None:
    assert format_refresh_timestamp(value) == "Not yet recorded"
