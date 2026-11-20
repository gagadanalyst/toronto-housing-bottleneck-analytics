{{ config(materialized='table') }}

WITH active_fact AS (
    SELECT
        CAST(COUNT(*) AS BIGINT) AS active_revisions_analysed,
        CAST(SUM(CASE WHEN COALESCE(builder_name_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS active_builder_name_conflict,
        CAST(SUM(CASE WHEN COALESCE(has_status_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS active_has_status_conflict,
        CAST(SUM(CASE WHEN COALESCE(application_date_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS active_application_date_conflict,
        CAST(SUM(CASE WHEN COALESCE(issued_date_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS active_issued_date_conflict,
        CAST(SUM(CASE WHEN COALESCE(geo_id_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS active_geo_id_conflict,
        CAST(SUM(CASE WHEN builder_name IS NULL AND NOT COALESCE(builder_name_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS active_builder_name_missing,
        CAST(SUM(CASE WHEN status IS NULL AND NOT COALESCE(has_status_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS active_status_missing,
        CAST(SUM(CASE WHEN application_date IS NULL AND NOT COALESCE(application_date_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS active_application_date_missing,
        CAST(SUM(CASE WHEN issued_date IS NULL AND NOT COALESCE(issued_date_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS active_issued_date_missing,
        CAST(SUM(CASE WHEN geo_id IS NULL AND NOT COALESCE(geo_id_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS active_geo_id_missing,
        CAST(SUM(CASE WHEN
            COALESCE(builder_name_conflict, FALSE)
            OR COALESCE(has_status_conflict, FALSE)
            OR COALESCE(application_date_conflict, FALSE)
            OR COALESCE(issued_date_conflict, FALSE)
            OR COALESCE(geo_id_conflict, FALSE)
            OR (status IS NULL AND NOT COALESCE(has_status_conflict, FALSE))
            OR (application_date IS NULL AND NOT COALESCE(application_date_conflict, FALSE))
            THEN 1 ELSE 0 END) AS BIGINT) AS active_revisions_with_findings
    FROM {{ ref('fct_active_permit_revision') }}
),

active_age AS (
    SELECT
        CAST(SUM(CASE WHEN is_age_eligible THEN 1 ELSE 0 END) AS BIGINT)
            AS active_revisions_age_eligible,
        CAST(SUM(CASE WHEN NOT is_age_eligible THEN 1 ELSE 0 END) AS BIGINT)
            AS active_revisions_age_ineligible
    FROM {{ ref('mart_active_permit_age') }}
),

active_exclusions AS (
    SELECT
        CAST(SUM(CASE WHEN cohort_exclusion_reason = 'status_conflict' THEN 1 ELSE 0 END) AS BIGINT)
            AS active_exclusion_status_conflict,
        CAST(SUM(CASE WHEN cohort_exclusion_reason = 'missing_status' THEN 1 ELSE 0 END) AS BIGINT)
            AS active_exclusion_missing_status,
        CAST(SUM(CASE WHEN cohort_exclusion_reason = 'terminal_or_exception_status' THEN 1 ELSE 0 END) AS BIGINT)
            AS active_exclusion_terminal_or_exception_status,
        CAST(SUM(CASE WHEN cohort_exclusion_reason = 'application_date_conflict' THEN 1 ELSE 0 END) AS BIGINT)
            AS active_exclusion_application_date_conflict,
        CAST(SUM(CASE WHEN cohort_exclusion_reason = 'missing_application_date' THEN 1 ELSE 0 END) AS BIGINT)
            AS active_exclusion_missing_application_date,
        CAST(SUM(CASE WHEN NOT is_age_eligible AND cohort_exclusion_reason IS NULL THEN 1 ELSE 0 END) AS BIGINT)
            AS active_exclusion_unclassified
    FROM {{ ref('mart_active_permit_age') }}
),

cleared_fact AS (
    SELECT
        CAST(COUNT(*) AS BIGINT) AS cleared_revision_count,
        CAST(SUM(CASE WHEN COALESCE(builder_name_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS builder_name_conflict,
        CAST(SUM(CASE WHEN COALESCE(has_status_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS has_status_conflict,
        CAST(SUM(CASE WHEN COALESCE(application_date_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS application_date_conflict,
        CAST(SUM(CASE WHEN COALESCE(issued_date_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS issued_date_conflict,
        CAST(SUM(CASE WHEN COALESCE(completed_date_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS completed_date_conflict,
        CAST(SUM(CASE WHEN COALESCE(geo_id_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS geo_id_conflict,
        CAST(SUM(CASE WHEN COALESCE(application_to_issue_negative, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS application_to_issue_negative,
        CAST(SUM(CASE WHEN COALESCE(application_to_completion_negative, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS application_to_completion_negative,
        CAST(SUM(CASE WHEN COALESCE(issue_to_completion_negative, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS issue_to_completion_negative,
        CAST(SUM(CASE WHEN application_date IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)
            AS application_date_usable,
        CAST(SUM(CASE WHEN application_date IS NULL AND NOT COALESCE(application_date_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS application_date_no_usable,
        CAST(SUM(CASE WHEN issued_date IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)
            AS issued_date_usable,
        CAST(SUM(CASE WHEN issued_date IS NULL AND NOT COALESCE(issued_date_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS issued_date_no_usable,
        CAST(SUM(CASE WHEN completed_date IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)
            AS completion_date_usable,
        CAST(SUM(CASE WHEN completed_date IS NULL AND NOT COALESCE(completed_date_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS completion_date_no_usable,
        CAST(SUM(CASE WHEN application_date IS NOT NULL AND completed_date IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)
            AS both_dates_available,
        CAST(SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) AS BIGINT)
            AS closed_revision_count,
        CAST(SUM(CASE WHEN status = 'Closed' AND application_date IS NOT NULL AND completed_date IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)
            AS closed_both_dates_available,
        CAST(SUM(CASE WHEN status IS NULL OR status <> 'Closed' THEN
            CASE WHEN application_date IS NOT NULL AND completed_date IS NOT NULL THEN 1 ELSE 0 END
            ELSE 0 END) AS BIGINT) AS non_closed_both_dates_available,
        CAST(SUM(CASE WHEN observed_builder_names IS NULL AND NOT COALESCE(builder_name_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS cleared_builder_name_missing,
        CAST(SUM(CASE WHEN status IS NULL AND NOT COALESCE(has_status_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS cleared_status_missing,
        CAST(SUM(CASE WHEN geo_id IS NULL AND NOT COALESCE(geo_id_conflict, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
            AS cleared_geo_id_missing,
        CAST(SUM(CASE WHEN
            COALESCE(builder_name_conflict, FALSE)
            OR COALESCE(has_status_conflict, FALSE)
            OR COALESCE(application_date_conflict, FALSE)
            OR COALESCE(issued_date_conflict, FALSE)
            OR COALESCE(completed_date_conflict, FALSE)
            OR COALESCE(geo_id_conflict, FALSE)
            OR COALESCE(application_to_issue_negative, FALSE)
            OR COALESCE(application_to_completion_negative, FALSE)
            OR COALESCE(issue_to_completion_negative, FALSE)
            OR (status IS NULL AND NOT COALESCE(has_status_conflict, FALSE))
            THEN 1 ELSE 0 END) AS BIGINT) AS cleared_revisions_with_findings
    FROM {{ ref('fct_cleared_permit_revision') }}
),

metrics AS (
    SELECT 'active_revisions_analysed' AS metric_name, active_revisions_analysed AS metric_value, 'active_summary' AS metric_group FROM active_fact
    UNION ALL SELECT 'active_revisions_age_eligible', active_revisions_age_eligible, 'active_summary' FROM active_age
    UNION ALL SELECT 'active_revisions_age_ineligible', active_revisions_age_ineligible, 'active_summary' FROM active_age
    UNION ALL SELECT 'active_builder_name_conflict', active_builder_name_conflict, 'active_checks' FROM active_fact
    UNION ALL SELECT 'active_has_status_conflict', active_has_status_conflict, 'active_checks' FROM active_fact
    UNION ALL SELECT 'active_application_date_conflict', active_application_date_conflict, 'active_checks' FROM active_fact
    UNION ALL SELECT 'active_issued_date_conflict', active_issued_date_conflict, 'active_checks' FROM active_fact
    UNION ALL SELECT 'active_geo_id_conflict', active_geo_id_conflict, 'active_checks' FROM active_fact
    UNION ALL SELECT 'active_builder_name_missing', active_builder_name_missing, 'active_checks' FROM active_fact
    UNION ALL SELECT 'active_status_missing', active_status_missing, 'active_checks' FROM active_fact
    UNION ALL SELECT 'active_application_date_missing', active_application_date_missing, 'active_checks' FROM active_fact
    UNION ALL SELECT 'active_issued_date_missing', active_issued_date_missing, 'active_checks' FROM active_fact
    UNION ALL SELECT 'active_geo_id_missing', active_geo_id_missing, 'active_checks' FROM active_fact
    UNION ALL SELECT 'active_revisions_with_findings', active_revisions_with_findings, 'active_summary' FROM active_fact
    UNION ALL SELECT 'active_exclusion_status_conflict', active_exclusion_status_conflict, 'active_exclusions' FROM active_exclusions
    UNION ALL SELECT 'active_exclusion_missing_status', active_exclusion_missing_status, 'active_exclusions' FROM active_exclusions
    UNION ALL SELECT 'active_exclusion_terminal_or_exception_status', active_exclusion_terminal_or_exception_status, 'active_exclusions' FROM active_exclusions
    UNION ALL SELECT 'active_exclusion_application_date_conflict', active_exclusion_application_date_conflict, 'active_exclusions' FROM active_exclusions
    UNION ALL SELECT 'active_exclusion_missing_application_date', active_exclusion_missing_application_date, 'active_exclusions' FROM active_exclusions
    UNION ALL SELECT 'active_exclusion_unclassified', active_exclusion_unclassified, 'active_exclusions' FROM active_exclusions
    UNION ALL SELECT 'cleared_revision_count', cleared_revision_count, 'cleared_summary' FROM cleared_fact
    UNION ALL SELECT 'builder_name_conflict', builder_name_conflict, 'cleared_checks' FROM cleared_fact
    UNION ALL SELECT 'has_status_conflict', has_status_conflict, 'cleared_checks' FROM cleared_fact
    UNION ALL SELECT 'application_date_conflict', application_date_conflict, 'cleared_checks' FROM cleared_fact
    UNION ALL SELECT 'issued_date_conflict', issued_date_conflict, 'cleared_checks' FROM cleared_fact
    UNION ALL SELECT 'completed_date_conflict', completed_date_conflict, 'cleared_checks' FROM cleared_fact
    UNION ALL SELECT 'geo_id_conflict', geo_id_conflict, 'cleared_checks' FROM cleared_fact
    UNION ALL SELECT 'application_to_issue_negative', application_to_issue_negative, 'cleared_checks' FROM cleared_fact
    UNION ALL SELECT 'application_to_completion_negative', application_to_completion_negative, 'cleared_checks' FROM cleared_fact
    UNION ALL SELECT 'issue_to_completion_negative', issue_to_completion_negative, 'cleared_checks' FROM cleared_fact
    UNION ALL SELECT 'application_date_usable', application_date_usable, 'cleared_coverage' FROM cleared_fact
    UNION ALL SELECT 'application_date_no_usable', application_date_no_usable, 'cleared_coverage' FROM cleared_fact
    UNION ALL SELECT 'issued_date_usable', issued_date_usable, 'cleared_coverage' FROM cleared_fact
    UNION ALL SELECT 'issued_date_no_usable', issued_date_no_usable, 'cleared_coverage' FROM cleared_fact
    UNION ALL SELECT 'completion_date_usable', completion_date_usable, 'cleared_coverage' FROM cleared_fact
    UNION ALL SELECT 'completion_date_no_usable', completion_date_no_usable, 'cleared_coverage' FROM cleared_fact
    UNION ALL SELECT 'both_dates_available', both_dates_available, 'cleared_coverage' FROM cleared_fact
    UNION ALL SELECT 'closed_revision_count', closed_revision_count, 'cleared_coverage' FROM cleared_fact
    UNION ALL SELECT 'closed_both_dates_available', closed_both_dates_available, 'cleared_coverage' FROM cleared_fact
    UNION ALL SELECT 'non_closed_both_dates_available', non_closed_both_dates_available, 'cleared_coverage' FROM cleared_fact
    UNION ALL SELECT 'cleared_builder_name_missing', cleared_builder_name_missing, 'cleared_checks' FROM cleared_fact
    UNION ALL SELECT 'cleared_status_missing', cleared_status_missing, 'cleared_checks' FROM cleared_fact
    UNION ALL SELECT 'cleared_geo_id_missing', cleared_geo_id_missing, 'cleared_checks' FROM cleared_fact
    UNION ALL SELECT 'cleared_revisions_with_findings', cleared_revisions_with_findings, 'cleared_summary' FROM cleared_fact
)

SELECT
    metric_name,
    CAST(metric_value AS BIGINT) AS metric_value,
    metric_group
FROM metrics
