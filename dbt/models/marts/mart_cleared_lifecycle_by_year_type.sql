{{ config(materialized='table') }}
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
    SELECT
        application_year,
        permit_type,
        'Application to issue' AS measure,
        application_to_issue_days_raw AS duration_days
    FROM closed_permits
    UNION ALL
    SELECT
        application_year,
        permit_type,
        'Application to completion',
        application_to_completion_days_raw
    FROM closed_permits
    UNION ALL
    SELECT
        application_year,
        permit_type,
        'Issue to completion',
        issue_to_completion_days_raw
    FROM closed_permits
),

summary AS (
    SELECT
        application_year,
        permit_type,
        measure,
        COUNT(duration_days) AS eligible_date_pairs,
        SUM(CASE WHEN duration_days < 0 THEN 1 ELSE 0 END)
            AS negative_interval_count,
        SUM(CASE WHEN duration_days >= 0 THEN 1 ELSE 0 END)
            AS publishable_duration_count,
        MEDIAN(CASE WHEN duration_days >= 0 THEN duration_days END)
            AS median_days,
        PERCENTILE_CONT(0.90) WITHIN GROUP (
            ORDER BY CASE WHEN duration_days >= 0 THEN duration_days END
        ) AS p90_days,
        MAX(CASE WHEN duration_days >= 0 THEN duration_days END) AS max_days
    FROM duration_rows
    WHERE application_year IS NOT NULL
    GROUP BY application_year, permit_type, measure
)

SELECT
    *,
    ROUND(
        100.0 * negative_interval_count
        / NULLIF(eligible_date_pairs, 0),
        2
    ) AS negative_interval_rate_pct
FROM summary
