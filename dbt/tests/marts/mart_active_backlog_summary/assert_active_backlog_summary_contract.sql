WITH model AS (
    SELECT * FROM {{ ref('mart_active_backlog_summary') }}
), failures AS (
    SELECT CAST(application_year AS INTEGER) AS application_year,
           CAST(permit_type AS VARCHAR) AS permit_type,
           'invalid_summary_row' AS failure_reason
    FROM model
    WHERE summary_level NOT IN ('overall', 'application_year', 'permit_type')
       OR current_active_revision_count < 0
       OR age_eligible_count < 0
       OR age_ineligible_count < 0
       OR current_active_revision_count <> age_eligible_count + age_ineligible_count
       OR over_1095_days_count > over_730_days_count
       OR over_730_days_count > over_365_days_count
       OR over_365_days_count > age_eligible_count
       OR median_age_days < 0
       OR p90_age_days < median_age_days
       OR (age_eligible_count = 0 AND (median_age_days IS NOT NULL OR p90_age_days IS NOT NULL))
       OR (age_eligible_count > 0 AND (median_age_days IS NULL OR p90_age_days IS NULL))
       OR (eligible_backlog_share_pct IS NOT NULL AND (eligible_backlog_share_pct < 0 OR eligible_backlog_share_pct > 100))

    UNION ALL

    SELECT CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR), 'duplicate_summary_group'
    FROM model
    GROUP BY summary_level, application_year, permit_type
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR), 'overall_row_count'
    WHERE (SELECT COUNT(*) FROM model WHERE summary_level = 'overall') <> 1

    UNION ALL

    SELECT CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR), 'year_shares_do_not_sum_to_100'
    WHERE (SELECT COALESCE(SUM(age_eligible_count), 0) FROM model WHERE summary_level = 'application_year') > 0
      AND ABS((SELECT SUM(eligible_backlog_share_pct) FROM model WHERE summary_level = 'application_year') - 100.0) > 0.0001

    UNION ALL

    SELECT CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR), 'type_shares_do_not_sum_to_100'
    WHERE (SELECT COALESCE(SUM(age_eligible_count), 0) FROM model WHERE summary_level = 'permit_type') > 0
      AND ABS((SELECT SUM(eligible_backlog_share_pct) FROM model WHERE summary_level = 'permit_type') - 100.0) > 0.0001

    UNION ALL

    SELECT CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR), 'overall_does_not_reconcile_to_year_groups'
    WHERE (SELECT current_active_revision_count FROM model WHERE summary_level = 'overall') <>
          (SELECT SUM(current_active_revision_count) FROM model WHERE summary_level = 'application_year')
)
SELECT * FROM failures
