WITH date_conversion_errors AS (
    SELECT
        source_row_version_id,
        'application_date' AS field_name,
        application_date_raw AS raw_value
    FROM {{ ref('stg_active_permits') }}
    WHERE NULLIF(TRIM(application_date_raw), '') IS NULL
       OR TRY_CAST(TRIM(application_date_raw) AS DATE) IS NULL
       OR application_date IS DISTINCT FROM
          TRY_CAST(TRIM(application_date_raw) AS DATE)

    UNION ALL

    SELECT
        source_row_version_id,
        'issued_date' AS field_name,
        issued_date_raw AS raw_value
    FROM {{ ref('stg_active_permits') }}
    WHERE (
        NULLIF(TRIM(issued_date_raw), '') IS NULL
        AND issued_date IS NOT NULL
    )
    OR (
        NULLIF(TRIM(issued_date_raw), '') IS NOT NULL
        AND (
            TRY_CAST(TRIM(issued_date_raw) AS DATE) IS NULL
            OR issued_date IS DISTINCT FROM
               TRY_CAST(TRIM(issued_date_raw) AS DATE)
        )
    )
)
SELECT *
FROM date_conversion_errors