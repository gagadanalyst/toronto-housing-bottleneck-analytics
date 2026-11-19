WITH model AS (
    SELECT * FROM {{ ref('mart_cleared_lifecycle_pooled') }}
), failures AS (
    SELECT measure, 'invalid_pooled_lifecycle_statistics' AS failure_reason
    FROM model
    WHERE matched_cohort_count <= 0
       OR median_days IS NULL
       OR p90_days IS NULL
       OR median_days < 0
       OR p90_days < median_days

    UNION ALL

    SELECT CAST(NULL AS VARCHAR), 'expected_three_lifecycle_measures'
    WHERE (SELECT COUNT(*) FROM model) <> 3
       OR (SELECT COUNT(DISTINCT measure) FROM model) <> 3

    UNION ALL

    SELECT CAST(NULL AS VARCHAR), 'matched_cohort_counts_differ'
    WHERE (SELECT MIN(matched_cohort_count) FROM model) <>
          (SELECT MAX(matched_cohort_count) FROM model)
)
SELECT * FROM failures
