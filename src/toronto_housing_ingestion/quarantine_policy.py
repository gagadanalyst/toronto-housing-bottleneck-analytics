"""Shared quarantine-volume threshold policy."""

from __future__ import annotations


def quarantine_threshold_breached(
    bad_row_count: int, baseline_current_rows: int, threshold: float = 0.25
) -> bool:
    """Return whether newly quarantined bad rows reach the configured ratio.

    Exact duplicate occurrences are excluded by callers from ``bad_row_count``.
    A positive bad-row count against an empty baseline is always a breach.
    The comparison is inclusive so exactly 25% blocks promotion.
    """
    if bad_row_count < 0 or baseline_current_rows < 0:
        raise ValueError("Quarantine counts and baselines cannot be negative.")
    if not 0 <= threshold <= 1:
        raise ValueError("Quarantine threshold must be between 0 and 1.")
    if bad_row_count == 0:
        return False
    if baseline_current_rows == 0:
        return True
    return bad_row_count / baseline_current_rows >= threshold


def quarantine_generation_breaches(
    bad_rows_by_source: dict[str, int],
    baseline_current_rows: dict[str, int],
    threshold: float = 0.25,
) -> dict[str, tuple[int, int]]:
    """Return source totals that breach the ratio for one candidate generation.

    Callers aggregate newly quarantined bad rows across every newly replayed
    archive in the candidate. Existing baseline archive IDs are excluded before
    calling this helper. This prevents a long backlog from evading the limit by
    splitting bad rows across individually small snapshots.
    """
    breaches: dict[str, tuple[int, int]] = {}
    for source_name, bad_row_count in bad_rows_by_source.items():
        baseline = baseline_current_rows.get(source_name, 0)
        if quarantine_threshold_breached(bad_row_count, baseline, threshold):
            breaches[source_name] = (bad_row_count, baseline)
    return breaches
