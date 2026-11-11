SELECT
    application_year,
    permit_type,
    'invalid_active_backlog_aggregation' AS failure_reason
FROM {{ ref('mart_active_backlog_by_year_type') }}
WHERE current_active_revision_count
          <> age_eligible_count + age_ineligible_count
   OR age_eligible_count < 0
   OR age_ineligible_count < 0
   OR over_365_days_count < 0
   OR over_730_days_count < 0
   OR over_1095_days_count < 0
   OR over_1095_days_count > over_730_days_count
   OR over_730_days_count > over_365_days_count
   OR over_365_days_count > age_eligible_count
   OR median_age_days < 0
   OR p90_age_days < 0
   OR (median_age_days IS NOT NULL AND p90_age_days < median_age_days)
   OR (
        age_eligible_count = 0
        AND (median_age_days IS NOT NULL OR p90_age_days IS NOT NULL)
   )
   OR (
        age_eligible_count > 0
        AND (median_age_days IS NULL OR p90_age_days IS NULL)
   )
   OR (
        eligible_backlog_share_pct IS NOT NULL
        AND (
            eligible_backlog_share_pct < 0
            OR eligible_backlog_share_pct > 100
        )
   )

UNION ALL

SELECT
    CAST(NULL AS INTEGER) AS application_year,
    CAST(NULL AS VARCHAR) AS permit_type,
    'eligible_backlog_share_does_not_sum_to_100' AS failure_reason
WHERE
    (SELECT COALESCE(SUM(age_eligible_count), 0)
     FROM {{ ref('mart_active_backlog_by_year_type') }}) > 0
    AND ABS(
        (SELECT SUM(eligible_backlog_share_pct)
         FROM {{ ref('mart_active_backlog_by_year_type') }}) - 100.0
    ) > 0.0001
