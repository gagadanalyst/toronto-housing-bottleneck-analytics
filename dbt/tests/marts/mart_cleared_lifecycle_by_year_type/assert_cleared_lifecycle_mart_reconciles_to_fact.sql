WITH closed_permits AS (
    SELECT
        CAST(EXTRACT(YEAR FROM application_date) AS INTEGER)
            AS application_year,
        permit_type,
        application_to_issue_days_raw,
        application_to_completion_days_raw,
        issue_to_completion_days_raw
    FROM {{ ref('fct_cleared_permit_revision') }}
    WHERE status = 'Closed'
),
duration_rows AS (
    SELECT application_year, permit_type,
        'Application to issue' AS measure,
        application_to_issue_days_raw AS duration_days
    FROM closed_permits
    UNION ALL
    SELECT application_year, permit_type,
        'Application to completion',
        application_to_completion_days_raw
    FROM closed_permits
    UNION ALL
    SELECT application_year, permit_type,
        'Issue to completion',
        issue_to_completion_days_raw
    FROM closed_permits
),
expected AS (
    SELECT
        application_year,
        permit_type,
        measure,
        COUNT(duration_days) AS eligible_date_pairs,
        SUM(CASE WHEN duration_days < 0 THEN 1 ELSE 0 END)
            AS negative_interval_count,
        SUM(CASE WHEN duration_days >= 0 THEN 1 ELSE 0 END)
            AS publishable_duration_count,
        ROUND(
            100.0 * SUM(CASE WHEN duration_days < 0 THEN 1 ELSE 0 END)
            / NULLIF(COUNT(duration_days), 0),
            2
        ) AS negative_interval_rate_pct,
        MEDIAN(CASE WHEN duration_days >= 0 THEN duration_days END)
            AS median_days,
        PERCENTILE_CONT(0.90) WITHIN GROUP (
            ORDER BY CASE WHEN duration_days >= 0
                THEN duration_days END
        ) AS p90_days,
        MAX(CASE WHEN duration_days >= 0 THEN duration_days END)
            AS max_days
    FROM duration_rows
    WHERE application_year IS NOT NULL
    GROUP BY application_year, permit_type, measure
),
actual AS (
    SELECT *
    FROM {{ ref('mart_cleared_lifecycle_by_year_type') }}
)
SELECT
    COALESCE(e.application_year, a.application_year)
        AS application_year,
    COALESCE(e.permit_type, a.permit_type) AS permit_type,
    COALESCE(e.measure, a.measure) AS measure,
    e.eligible_date_pairs AS expected_eligible_date_pairs,
    a.eligible_date_pairs AS actual_eligible_date_pairs,
    e.negative_interval_count AS expected_negative_count,
    a.negative_interval_count AS actual_negative_count,
    e.publishable_duration_count AS expected_publishable_count,
    a.publishable_duration_count AS actual_publishable_count,
    e.negative_interval_rate_pct AS expected_negative_rate_pct,
    a.negative_interval_rate_pct AS actual_negative_rate_pct,
    e.median_days AS expected_median_days,
    a.median_days AS actual_median_days,
    e.p90_days AS expected_p90_days,
    a.p90_days AS actual_p90_days,
    e.max_days AS expected_max_days,
    a.max_days AS actual_max_days
FROM expected AS e
FULL OUTER JOIN actual AS a
    USING (application_year, permit_type, measure)
WHERE
    e.application_year IS NULL
    OR a.application_year IS NULL
    OR e.eligible_date_pairs IS DISTINCT FROM a.eligible_date_pairs
    OR e.negative_interval_count
        IS DISTINCT FROM a.negative_interval_count
    OR e.publishable_duration_count
        IS DISTINCT FROM a.publishable_duration_count
    OR e.negative_interval_rate_pct
        IS DISTINCT FROM a.negative_interval_rate_pct
    OR e.median_days IS DISTINCT FROM a.median_days
    OR e.p90_days IS DISTINCT FROM a.p90_days
    OR e.max_days IS DISTINCT FROM a.max_days
