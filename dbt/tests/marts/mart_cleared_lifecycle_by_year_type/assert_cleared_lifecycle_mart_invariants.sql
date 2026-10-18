SELECT
    application_year,
    permit_type,
    measure
FROM {{ ref('mart_cleared_lifecycle_by_year_type') }}
WHERE
    eligible_date_pairs IS DISTINCT FROM
        negative_interval_count + publishable_duration_count
    OR negative_interval_rate_pct IS DISTINCT FROM ROUND(
        100.0 * negative_interval_count
        / NULLIF(eligible_date_pairs, 0),
        2
    )
    OR median_days < 0
    OR p90_days < 0
    OR max_days < 0
    OR (
        publishable_duration_count = 0
        AND (
            median_days IS NOT NULL
            OR p90_days IS NOT NULL
            OR max_days IS NOT NULL
        )
    )
    OR (
        publishable_duration_count > 0
        AND (
            median_days IS NULL
            OR p90_days IS NULL
            OR max_days IS NULL
        )
    )
    OR (median_days IS NOT NULL AND p90_days < median_days)
    OR (p90_days IS NOT NULL AND max_days < p90_days)
