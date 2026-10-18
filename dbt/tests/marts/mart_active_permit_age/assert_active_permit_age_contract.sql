WITH fact AS (
    SELECT *
    FROM {{ ref('fct_active_permit_revision') }}
),

expected AS (
    SELECT
        f.*,
        CASE
            WHEN f.has_status_conflict
                THEN 'status_conflict'
            WHEN NULLIF(TRIM(f.status), '') IS NULL
                THEN 'missing_status'
            WHEN LOWER(TRIM(f.status))
                IN ('abandoned', 'cancelled', 'closed')
                THEN 'terminal_or_exception_status'
            WHEN f.application_date_conflict
                THEN 'application_date_conflict'
            WHEN f.application_date IS NULL
                THEN 'missing_application_date'
            ELSE NULL
        END AS expected_exclusion_reason
    FROM fact AS f
),

mart AS (
    SELECT *
    FROM {{ ref('mart_active_permit_age') }}
),

failures AS (

    SELECT
        permit_num, revision_num, permit_type,
        'duplicate_mart_key' AS failed_contract
    FROM mart
    GROUP BY permit_num, revision_num, permit_type
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        COALESCE(e.permit_num, m.permit_num),
        COALESCE(e.revision_num, m.revision_num),
        COALESCE(e.permit_type, m.permit_type),
        'fact_mart_row_coverage'
    FROM expected AS e
    FULL OUTER JOIN mart AS m
        USING (permit_num, revision_num, permit_type)
    WHERE e.permit_num IS NULL
       OR m.permit_num IS NULL

    UNION ALL

    SELECT
        e.permit_num, e.revision_num, e.permit_type,
        'cohort_eligibility'
    FROM expected AS e
    JOIN mart AS m
        USING (permit_num, revision_num, permit_type)
    WHERE m.cohort_exclusion_reason
              IS DISTINCT FROM e.expected_exclusion_reason
       OR m.is_age_eligible
              IS DISTINCT FROM (e.expected_exclusion_reason IS NULL)

    UNION ALL

    SELECT
        e.permit_num, e.revision_num, e.permit_type,
        'status_normalization'
    FROM expected AS e
    JOIN mart AS m
        USING (permit_num, revision_num, permit_type)
    WHERE m.status_normalized
              IS DISTINCT FROM NULLIF(LOWER(TRIM(e.status)), '')

    UNION ALL

    SELECT
        e.permit_num, e.revision_num, e.permit_type,
        'as_of_date_missing'
    FROM expected AS e
    JOIN mart AS m
        USING (permit_num, revision_num, permit_type)
    WHERE m.as_of_date IS NULL

    UNION ALL

    SELECT
        e.permit_num, e.revision_num, e.permit_type,
        'eligible_age_invalid'
    FROM expected AS e
    JOIN mart AS m
        USING (permit_num, revision_num, permit_type)
    WHERE m.is_age_eligible
      AND (
          m.age_since_application_days IS NULL
          OR m.age_since_application_days < 0
          OR m.age_since_application_days
              IS DISTINCT FROM DATE_DIFF(
                  'day', e.application_date, m.as_of_date
              )
      )

    UNION ALL

    SELECT
        e.permit_num, e.revision_num, e.permit_type,
        'ineligible_age_published'
    FROM expected AS e
    JOIN mart AS m
        USING (permit_num, revision_num, permit_type)
    WHERE NOT m.is_age_eligible
      AND m.age_since_application_days IS NOT NULL
)

SELECT *
FROM failures