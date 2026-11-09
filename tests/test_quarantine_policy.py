import pytest

from toronto_housing_ingestion.quarantine_policy import (
    quarantine_generation_breaches,
    quarantine_threshold_breached,
)


@pytest.mark.parametrize(
    ("bad_rows", "baseline", "expected"),
    [
        (0, 100, False),
        (24, 100, False),
        (25, 100, True),
        (26, 100, True),
        (1, 0, True),
        (0, 0, False),
        (1, 1, True),
    ],
)
def test_quarantine_threshold_is_inclusive_and_uses_pre_run_current_rows(
    bad_rows: int, baseline: int, expected: bool
) -> None:
    assert quarantine_threshold_breached(bad_rows, baseline) is expected


def test_quarantine_threshold_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        quarantine_threshold_breached(-1, 100)
    with pytest.raises(ValueError, match="between 0 and 1"):
        quarantine_threshold_breached(1, 100, 1.1)


def test_generation_threshold_aggregates_bad_rows_across_new_archives() -> None:
    breaches = quarantine_generation_breaches(
        {"active_permits": 10 + 15, "cleared_permits": 8},
        {"active_permits": 100, "cleared_permits": 100},
    )

    assert breaches == {"active_permits": (25, 100)}


def test_generation_threshold_is_independent_for_each_source() -> None:
    breaches = quarantine_generation_breaches(
        {"active_permits": 24, "cleared_permits": 25},
        {"active_permits": 100, "cleared_permits": 100},
    )

    assert breaches == {"cleared_permits": (25, 100)}
