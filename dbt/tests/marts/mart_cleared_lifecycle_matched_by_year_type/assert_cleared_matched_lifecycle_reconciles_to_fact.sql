WITH matched_closed_cohort AS (
    SELECT
        CAST(EXTRACT(YEAR FROM application_date) AS INTEGER)
            AS application_year,
        permit_type,
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
        application_year,
        permit_type,
        'Application to issue' AS measure,
        application_to_issue_days_raw AS duration_days
    FROM matched_closed_cohort

    UNION ALL

    SELECT
        application_year,
        permit_type,
        'Application to completion' AS measure,
        application_to_completion_days_raw AS duration_days
    FROM matched_closed_cohort

    UNION ALL

    SELECT
        application_year,
        permit_type,
        'Issue to completion' AS measure,
        issue_to_completion_days_raw AS duration_days
    FROM matched_closed_cohort
),

expected AS (
    SELECT
        application_year,
        permit_type,
        measure,
        CAST(COUNT(*) AS BIGINT) AS matched_cohort_count,
        CAST(MEDIAN(duration_days) AS DOUBLE) AS median_days,
        CAST(
            ROUND(
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY duration_days),
                2
            ) AS DOUBLE
        ) AS p90_days
    FROM duration_rows
    GROUP BY application_year, permit_type, measure
),

actual AS (
    SELECT *
    FROM {{ ref('mart_cleared_lifecycle_matched_by_year_type') }}
)

SELECT
    e.application_year AS expected_application_year,
    a.application_year AS actual_application_year,
    e.permit_type AS expected_permit_type,
    a.permit_type AS actual_permit_type,
    e.measure AS expected_measure,
    a.measure AS actual_measure
FROM expected AS e
FULL OUTER JOIN actual AS a
    ON e.application_year = a.application_year
   AND e.permit_type = a.permit_type
   AND e.measure = a.measure
WHERE
    e.application_year IS NULL
    OR a.application_year IS NULL
    OR e.matched_cohort_count IS DISTINCT FROM a.matched_cohort_count
    OR e.median_days IS DISTINCT FROM a.median_days
    OR e.p90_days IS DISTINCT FROM a.p90_days
