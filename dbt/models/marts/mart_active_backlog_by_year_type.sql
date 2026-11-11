{{ config(materialized='table') }}

WITH active_revisions AS (
    SELECT
        CAST(EXTRACT(YEAR FROM application_date) AS INTEGER)
            AS application_year,
        permit_type,
        is_age_eligible,
        age_since_application_days
    FROM {{ ref('mart_active_permit_age') }}
),

summary AS (
    SELECT
        application_year,
        permit_type,
        CAST(COUNT(*) AS BIGINT) AS current_active_revision_count,
        CAST(
            SUM(CASE WHEN is_age_eligible THEN 1 ELSE 0 END)
            AS BIGINT
        ) AS age_eligible_count,
        CAST(
            SUM(CASE WHEN NOT is_age_eligible THEN 1 ELSE 0 END)
            AS BIGINT
        ) AS age_ineligible_count,
        CAST(
            SUM(
                CASE
                    WHEN is_age_eligible
                        AND age_since_application_days > 365
                    THEN 1 ELSE 0
                END
            ) AS BIGINT
        ) AS over_365_days_count,
        CAST(
            SUM(
                CASE
                    WHEN is_age_eligible
                        AND age_since_application_days > 730
                    THEN 1 ELSE 0
                END
            ) AS BIGINT
        ) AS over_730_days_count,
        CAST(
            SUM(
                CASE
                    WHEN is_age_eligible
                        AND age_since_application_days > 1095
                    THEN 1 ELSE 0
                END
            ) AS BIGINT
        ) AS over_1095_days_count,
        CAST(
            MEDIAN(
                CASE
                    WHEN is_age_eligible THEN age_since_application_days
                END
            ) AS DOUBLE
        ) AS median_age_days,
        CAST(
            ROUND(
                PERCENTILE_CONT(0.90) WITHIN GROUP (
                    ORDER BY CASE
                        WHEN is_age_eligible THEN age_since_application_days
                    END
                ),
                2
            ) AS DOUBLE
        ) AS p90_age_days
    FROM active_revisions
    GROUP BY application_year, permit_type
)

SELECT
    *,
    CAST(
        100.0 * age_eligible_count
        / NULLIF(SUM(age_eligible_count) OVER (), 0)
        AS DOUBLE
    ) AS eligible_backlog_share_pct
FROM summary
