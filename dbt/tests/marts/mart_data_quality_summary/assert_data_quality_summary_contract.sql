WITH model AS (
    SELECT * FROM {{ ref('mart_data_quality_summary') }}
), failures AS (
    SELECT CAST(metric_name AS VARCHAR) AS metric_name, 'duplicate_metric' AS failure_reason
    FROM model
    GROUP BY metric_name
    HAVING COUNT(*) <> 1

    UNION ALL

    SELECT metric_name, 'negative_metric_value'
    FROM model
    WHERE metric_value < 0

    UNION ALL

    SELECT expected.metric_name, 'required_metric_missing'
    FROM (
        SELECT 'active_revisions_analysed' AS metric_name
        UNION ALL SELECT 'active_revisions_age_eligible'
        UNION ALL SELECT 'active_revisions_age_ineligible'
        UNION ALL SELECT 'active_builder_name_conflict'
        UNION ALL SELECT 'active_has_status_conflict'
        UNION ALL SELECT 'active_application_date_conflict'
        UNION ALL SELECT 'active_issued_date_conflict'
        UNION ALL SELECT 'active_geo_id_conflict'
        UNION ALL SELECT 'active_builder_name_missing'
        UNION ALL SELECT 'active_status_missing'
        UNION ALL SELECT 'active_application_date_missing'
        UNION ALL SELECT 'active_issued_date_missing'
        UNION ALL SELECT 'active_geo_id_missing'
        UNION ALL SELECT 'active_revisions_with_findings'
        UNION ALL SELECT 'active_exclusion_status_conflict'
        UNION ALL SELECT 'active_exclusion_missing_status'
        UNION ALL SELECT 'active_exclusion_terminal_or_exception_status'
        UNION ALL SELECT 'active_exclusion_application_date_conflict'
        UNION ALL SELECT 'active_exclusion_missing_application_date'
        UNION ALL SELECT 'active_exclusion_unclassified'
        UNION ALL SELECT 'cleared_revision_count'
        UNION ALL SELECT 'builder_name_conflict'
        UNION ALL SELECT 'has_status_conflict'
        UNION ALL SELECT 'application_date_conflict'
        UNION ALL SELECT 'issued_date_conflict'
        UNION ALL SELECT 'completed_date_conflict'
        UNION ALL SELECT 'geo_id_conflict'
        UNION ALL SELECT 'application_to_issue_negative'
        UNION ALL SELECT 'application_to_completion_negative'
        UNION ALL SELECT 'issue_to_completion_negative'
        UNION ALL SELECT 'application_date_usable'
        UNION ALL SELECT 'application_date_no_usable'
        UNION ALL SELECT 'issued_date_usable'
        UNION ALL SELECT 'issued_date_no_usable'
        UNION ALL SELECT 'completion_date_usable'
        UNION ALL SELECT 'completion_date_no_usable'
        UNION ALL SELECT 'both_dates_available'
        UNION ALL SELECT 'closed_revision_count'
        UNION ALL SELECT 'closed_matched_three_date_count'
        UNION ALL SELECT 'closed_excluded_conflicting_date_count'
        UNION ALL SELECT 'closed_excluded_missing_date_count'
        UNION ALL SELECT 'closed_excluded_out_of_order_date_count'
        UNION ALL SELECT 'closed_both_dates_available'
        UNION ALL SELECT 'non_closed_both_dates_available'
        UNION ALL SELECT 'cleared_builder_name_missing'
        UNION ALL SELECT 'cleared_status_missing'
        UNION ALL SELECT 'cleared_geo_id_missing'
        UNION ALL SELECT 'cleared_revisions_with_findings'
    ) AS expected
    LEFT JOIN model USING (metric_name)
    WHERE model.metric_name IS NULL

    UNION ALL

    SELECT CAST('active_exclusion_counts' AS VARCHAR), 'exclusions_do_not_reconcile'
    WHERE (SELECT metric_value FROM model WHERE metric_name = 'active_revisions_age_ineligible') <>
          (SELECT COALESCE(SUM(metric_value), 0) FROM model WHERE metric_group = 'active_exclusions')

    UNION ALL

    SELECT CAST('active_revision_count' AS VARCHAR), 'active_fact_and_age_mart_do_not_reconcile'
    WHERE (SELECT metric_value FROM model WHERE metric_name = 'active_revisions_analysed') <>
          (SELECT metric_value FROM model WHERE metric_name = 'active_revisions_age_eligible') +
          (SELECT metric_value FROM model WHERE metric_name = 'active_revisions_age_ineligible')

    UNION ALL

    SELECT CAST('closed_both_dates_available' AS VARCHAR), 'closed_date_count_does_not_reconcile_to_lifecycle_mart'
    WHERE (SELECT metric_value FROM model WHERE metric_name = 'closed_both_dates_available') <>
          (SELECT COALESCE(SUM(eligible_date_pairs), 0)
           FROM {{ ref('mart_cleared_lifecycle_by_year_type') }}
           WHERE measure = 'Application to completion')

    UNION ALL

    SELECT CAST('closed_revision_count' AS VARCHAR), 'matched_closed_cohort_does_not_reconcile'
    WHERE (SELECT metric_value FROM model WHERE metric_name = 'closed_revision_count') <>
          (SELECT COALESCE(SUM(metric_value), 0)
           FROM model WHERE metric_name IN (
               'closed_matched_three_date_count',
               'closed_excluded_conflicting_date_count',
               'closed_excluded_missing_date_count',
               'closed_excluded_out_of_order_date_count'
           ))

    UNION ALL

    SELECT CAST('closed_matched_three_date_count' AS VARCHAR), 'matched_count_does_not_reconcile_to_matched_lifecycle_mart'
    WHERE (SELECT metric_value FROM model WHERE metric_name = 'closed_matched_three_date_count') <>
          (SELECT COALESCE(SUM(matched_cohort_count), 0)
           FROM {{ ref('mart_cleared_lifecycle_matched_by_year_type') }}
           WHERE measure = 'Application to completion')
)
SELECT * FROM failures
