{{ config(materialized='table') }}

WITH active_revisions AS (
    SELECT
        CAST(EXTRACT(YEAR FROM application_date) AS INTEGER) AS application_year,
        permit_type,
        is_age_eligible,
        age_since_application_days
    FROM {{ ref('mart_active_permit_age') }}
),

year_summary AS (
    SELECT
        application_year,
        CAST(COUNT(*) AS BIGINT) AS current_active_revision_count,
        CAST(SUM(CASE WHEN is_age_eligible THEN 1 ELSE 0 END) AS BIGINT)
            AS age_eligible_count,
        CAST(SUM(CASE WHEN NOT is_age_eligible THEN 1 ELSE 0 END) AS BIGINT)
            AS age_ineligible_count,
        CAST(SUM(CASE WHEN is_age_eligible AND age_since_application_days > 365 THEN 1 ELSE 0 END) AS BIGINT)
            AS over_365_days_count,
        CAST(SUM(CASE WHEN is_age_eligible AND age_since_application_days > 730 THEN 1 ELSE 0 END) AS BIGINT)
            AS over_730_days_count,
        CAST(SUM(CASE WHEN is_age_eligible AND age_since_application_days > 1095 THEN 1 ELSE 0 END) AS BIGINT)
            AS over_1095_days_count,
        CAST(MEDIAN(CASE WHEN is_age_eligible THEN age_since_application_days END) AS DOUBLE)
            AS median_age_days,
        CAST(ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (
            ORDER BY CASE WHEN is_age_eligible THEN age_since_application_days END
        ), 2) AS DOUBLE) AS p90_age_days
    FROM active_revisions
    GROUP BY application_year
),

type_summary AS (
    SELECT
        permit_type,
        CAST(COUNT(*) AS BIGINT) AS current_active_revision_count,
        CAST(SUM(CASE WHEN is_age_eligible THEN 1 ELSE 0 END) AS BIGINT)
            AS age_eligible_count,
        CAST(SUM(CASE WHEN NOT is_age_eligible THEN 1 ELSE 0 END) AS BIGINT)
            AS age_ineligible_count,
        CAST(SUM(CASE WHEN is_age_eligible AND age_since_application_days > 365 THEN 1 ELSE 0 END) AS BIGINT)
            AS over_365_days_count,
        CAST(SUM(CASE WHEN is_age_eligible AND age_since_application_days > 730 THEN 1 ELSE 0 END) AS BIGINT)
            AS over_730_days_count,
        CAST(SUM(CASE WHEN is_age_eligible AND age_since_application_days > 1095 THEN 1 ELSE 0 END) AS BIGINT)
            AS over_1095_days_count,
        CAST(MEDIAN(CASE WHEN is_age_eligible THEN age_since_application_days END) AS DOUBLE)
            AS median_age_days,
        CAST(ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (
            ORDER BY CASE WHEN is_age_eligible THEN age_since_application_days END
        ), 2) AS DOUBLE) AS p90_age_days
    FROM active_revisions
    GROUP BY permit_type
),

overall_summary AS (
    SELECT
        CAST(COUNT(*) AS BIGINT) AS current_active_revision_count,
        CAST(SUM(CASE WHEN is_age_eligible THEN 1 ELSE 0 END) AS BIGINT)
            AS age_eligible_count,
        CAST(SUM(CASE WHEN NOT is_age_eligible THEN 1 ELSE 0 END) AS BIGINT)
            AS age_ineligible_count,
        CAST(SUM(CASE WHEN is_age_eligible AND age_since_application_days > 365 THEN 1 ELSE 0 END) AS BIGINT)
            AS over_365_days_count,
        CAST(SUM(CASE WHEN is_age_eligible AND age_since_application_days > 730 THEN 1 ELSE 0 END) AS BIGINT)
            AS over_730_days_count,
        CAST(SUM(CASE WHEN is_age_eligible AND age_since_application_days > 1095 THEN 1 ELSE 0 END) AS BIGINT)
            AS over_1095_days_count,
        CAST(MEDIAN(CASE WHEN is_age_eligible THEN age_since_application_days END) AS DOUBLE)
            AS median_age_days,
        CAST(ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (
            ORDER BY CASE WHEN is_age_eligible THEN age_since_application_days END
        ), 2) AS DOUBLE) AS p90_age_days,
        CAST(MAX(application_year) AS INTEGER) AS latest_application_year
    FROM active_revisions
),

recent_cohort AS (
    SELECT
        CAST(100.0 * SUM(CASE WHEN application_year >= 2024 AND is_age_eligible THEN 1 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN is_age_eligible THEN 1 ELSE 0 END), 0) AS DOUBLE)
            AS recent_cohort_2024_plus_share_pct
    FROM active_revisions
),

top_five AS (
    SELECT
        CAST(100.0 * SUM(over_365_days_count)
            / NULLIF((SELECT SUM(over_365_days_count) FROM type_summary), 0) AS DOUBLE)
            AS top_five_over_365_share_pct
    FROM (
        SELECT over_365_days_count
        FROM type_summary
        ORDER BY over_365_days_count DESC, current_active_revision_count DESC, permit_type
        LIMIT 5
    ) AS ranked_types
),

summary_rows AS (
    SELECT
        'application_year' AS summary_level,
        application_year,
        CAST(NULL AS VARCHAR) AS permit_type,
        current_active_revision_count,
        age_eligible_count,
        age_ineligible_count,
        over_365_days_count,
        over_730_days_count,
        over_1095_days_count,
        median_age_days,
        p90_age_days
    FROM year_summary

    UNION ALL

    SELECT
        'permit_type',
        CAST(NULL AS INTEGER),
        permit_type,
        current_active_revision_count,
        age_eligible_count,
        age_ineligible_count,
        over_365_days_count,
        over_730_days_count,
        over_1095_days_count,
        median_age_days,
        p90_age_days
    FROM type_summary
)

SELECT
    summary_rows.summary_level,
    summary_rows.application_year,
    summary_rows.permit_type,
    summary_rows.current_active_revision_count,
    summary_rows.age_eligible_count,
    summary_rows.age_ineligible_count,
    summary_rows.over_365_days_count,
    summary_rows.over_730_days_count,
    summary_rows.over_1095_days_count,
    summary_rows.median_age_days,
    summary_rows.p90_age_days,
    CAST(100.0 * summary_rows.age_eligible_count / NULLIF(overall.age_eligible_count, 0) AS DOUBLE)
        AS eligible_backlog_share_pct,
    CAST(NULL AS DOUBLE) AS recent_cohort_2024_plus_share_pct,
    CAST(NULL AS DOUBLE) AS top_five_over_365_share_pct,
    CAST(NULL AS INTEGER) AS latest_application_year
FROM summary_rows
CROSS JOIN overall_summary AS overall

UNION ALL

SELECT
    'overall',
    CAST(NULL AS INTEGER),
    CAST(NULL AS VARCHAR),
    overall.current_active_revision_count,
    overall.age_eligible_count,
    overall.age_ineligible_count,
    overall.over_365_days_count,
    overall.over_730_days_count,
    overall.over_1095_days_count,
    overall.median_age_days,
    overall.p90_age_days,
    CASE WHEN overall.age_eligible_count > 0 THEN 100.0 ELSE 0.0 END,
    COALESCE(recent.recent_cohort_2024_plus_share_pct, 0.0),
    COALESCE(top5.top_five_over_365_share_pct, 0.0),
    overall.latest_application_year
FROM overall_summary AS overall
CROSS JOIN recent_cohort AS recent
CROSS JOIN top_five AS top5
