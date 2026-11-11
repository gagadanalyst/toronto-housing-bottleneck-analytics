WITH grouped AS (
    SELECT
        application_year,
        permit_type,
        COUNT(*) AS measure_rows,
        COUNT(DISTINCT measure) AS distinct_measures,
        MIN(matched_cohort_count) AS min_matched_cohort_count,
        MAX(matched_cohort_count) AS max_matched_cohort_count
    FROM {{ ref('mart_cleared_lifecycle_matched_by_year_type') }}
    GROUP BY application_year, permit_type
),

failures AS (
    SELECT
        application_year,
        permit_type,
        'matched_cohort_measure_coverage' AS failure_reason
    FROM grouped
    WHERE measure_rows <> 3
       OR distinct_measures <> 3
       OR min_matched_cohort_count <> max_matched_cohort_count

    UNION ALL

    SELECT
        application_year,
        permit_type,
        'invalid_matched_lifecycle_duration_statistics'
    FROM {{ ref('mart_cleared_lifecycle_matched_by_year_type') }}
    WHERE matched_cohort_count <= 0
       OR median_days IS NULL
       OR p90_days IS NULL
       OR median_days < 0
       OR p90_days < 0
       OR p90_days < median_days
)

SELECT *
FROM failures
