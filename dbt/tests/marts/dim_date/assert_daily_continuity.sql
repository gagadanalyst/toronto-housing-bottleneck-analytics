WITH ordered_dates AS (
    SELECT
        date_day,
        LAG(date_day) OVER (ORDER BY date_day) AS previous_date
    FROM {{ ref('dim_date') }}
)

SELECT
    previous_date,
    date_day
FROM ordered_dates
WHERE
    previous_date IS NOT NULL
    AND {{ date_diff_days('previous_date', 'date_day') }} <> 1
