{{ config(materialized='table') }}

WITH matched_closed_cohort AS (
    SELECT
        application_to_issue_days_raw,
        application_to_completion_days_raw,
        issue_to_completion_days_raw
    FROM {{ ref('fct_cleared_permit_revision') }}
    WHERE status = 'Closed'
      AND application_date IS NOT NULL
      AND issued_date IS NOT NULL
      AND completed_date IS NOT NULL
      AND application_date <= issued_date
      AND issued_date <= completed_date
),

duration_rows AS (
    SELECT
        'Application to issue' AS measure,
        application_to_issue_days_raw AS duration_days
    FROM matched_closed_cohort

    UNION ALL

    SELECT
        'Issue to completion',
        issue_to_completion_days_raw
    FROM matched_closed_cohort

    UNION ALL

    SELECT
        'Application to completion',
        application_to_completion_days_raw
    FROM matched_closed_cohort
)

SELECT
    measure,
    CAST(COUNT(*) AS BIGINT) AS matched_cohort_count,
    CAST(MEDIAN(duration_days) AS DOUBLE) AS median_days,
    CAST(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY duration_days) AS DOUBLE)
        AS p90_days
FROM duration_rows
GROUP BY measure
