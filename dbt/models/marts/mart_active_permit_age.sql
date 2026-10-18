WITH as_of_context AS (
    SELECT CAST(
        '{{ var("as_of_date", run_started_at.strftime("%Y-%m-%d")) }}'
        AS DATE
    ) AS as_of_date
),

fact AS (
    SELECT *
    FROM {{ ref('fct_active_permit_revision') }}
),

classified AS (
    SELECT
        f.*,
        NULLIF(LOWER(TRIM(f.status)), '') AS status_normalized,
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
        END AS cohort_exclusion_reason
    FROM fact AS f
)

SELECT
    c.permit_num,
    c.revision_num,
    c.permit_type,
    c.builder_name,
    c.status,
    c.status_normalized,
    c.application_date,
    c.issued_date,
    c.geo_id,
    c.current_source_row_count,
    c.has_status_conflict,
    c.application_date_conflict,
    c.issued_date_conflict,
    c.geo_id_conflict,
    a.as_of_date,
    c.cohort_exclusion_reason,
    c.cohort_exclusion_reason IS NULL AS is_age_eligible,
    CASE
        WHEN c.cohort_exclusion_reason IS NULL
        THEN DATE_DIFF('day', c.application_date, a.as_of_date)
        ELSE NULL
    END AS age_since_application_days
FROM classified AS c
CROSS JOIN as_of_context AS a