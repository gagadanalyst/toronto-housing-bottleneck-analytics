"""Format backend refresh timestamps consistently for the dashboard."""

from __future__ import annotations

from typing import Any

import pandas as pd


def format_refresh_timestamp(value: Any) -> str:
    """Convert a UTC timestamp to Toronto local time (ET) for display."""
    if value is None:
        return "Not yet recorded"

    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return "Not yet recorded"

    return timestamp.tz_convert("America/Toronto").strftime("%d %b %Y · %I:%M %p")
