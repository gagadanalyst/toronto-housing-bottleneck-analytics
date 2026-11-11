WITH expected_groups AS (
    SELECT
        CAST(EXTRACT(YEAR FROM application_date) AS INTEGER)
            AS application_year,
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
    FROM {{ ref('mart_active_permit_age') }}
    GROUP BY
        CAST(EXTRACT(YEAR FROM application_date) AS INTEGER),
        permit_type
),

expected AS (
    SELECT
        *,
        CAST(
            100.0 * age_eligible_count
            / NULLIF(SUM(age_eligible_count) OVER (), 0)
            AS DOUBLE
        ) AS eligible_backlog_share_pct
    FROM expected_groups
),

actual AS (
    SELECT *
    FROM {{ ref('mart_active_backlog_by_year_type') }}
)

SELECT
    e.application_year AS expected_application_year,
    a.application_year AS actual_application_year,
    e.permit_type AS expected_permit_type,
    a.permit_type AS actual_permit_type
FROM expected AS e
FULL OUTER JOIN actual AS a
    ON e.application_year IS NOT DISTINCT FROM a.application_year
   AND e.permit_type IS NOT DISTINCT FROM a.permit_type
WHERE
    e.permit_type IS NULL
    OR a.permit_type IS NULL
    OR e.current_active_revision_count
        IS DISTINCT FROM a.current_active_revision_count
    OR e.age_eligible_count IS DISTINCT FROM a.age_eligible_count
    OR e.age_ineligible_count IS DISTINCT FROM a.age_ineligible_count
    OR e.over_365_days_count IS DISTINCT FROM a.over_365_days_count
    OR e.over_730_days_count IS DISTINCT FROM a.over_730_days_count
    OR e.over_1095_days_count IS DISTINCT FROM a.over_1095_days_count
    OR e.median_age_days IS DISTINCT FROM a.median_age_days
    OR e.p90_age_days IS DISTINCT FROM a.p90_age_days
    OR e.eligible_backlog_share_pct
        IS DISTINCT FROM a.eligible_backlog_share_pct
