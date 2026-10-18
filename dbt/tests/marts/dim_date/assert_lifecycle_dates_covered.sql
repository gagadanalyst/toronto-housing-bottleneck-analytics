WITH lifecycle_dates AS (
    SELECT application_date AS lifecycle_date
    FROM {{ ref('fct_cleared_permit_revision') }}
    WHERE application_date IS NOT NULL
    UNION
    SELECT issued_date
    FROM {{ ref('fct_cleared_permit_revision') }}
    WHERE issued_date IS NOT NULL
    UNION
    SELECT completed_date
    FROM {{ ref('fct_cleared_permit_revision') }}
    WHERE completed_date IS NOT NULL
)

SELECT lifecycle_date
FROM lifecycle_dates AS l
LEFT JOIN {{ ref('dim_date') }} AS d
    ON l.lifecycle_date = d.date_day
WHERE d.date_day IS NULL
