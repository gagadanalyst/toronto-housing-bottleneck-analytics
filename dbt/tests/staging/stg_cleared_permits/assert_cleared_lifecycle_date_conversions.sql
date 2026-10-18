SELECT
    source_uniq_id,
    'application_date' AS field_name,
    application_date_raw AS raw_value
FROM {{ ref('stg_cleared_permits') }}
WHERE
    application_date_raw IS NOT NULL
    AND TRIM(application_date_raw) <> ''
    AND application_date IS NULL
UNION ALL
SELECT
    source_uniq_id,
    'issued_date' AS field_name,
    issued_date_raw AS raw_value
FROM {{ ref('stg_cleared_permits') }}
WHERE
    issued_date_raw IS NOT NULL
    AND TRIM(issued_date_raw) <> ''
    AND issued_date IS NULL
