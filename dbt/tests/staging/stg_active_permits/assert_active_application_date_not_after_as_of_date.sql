SELECT *
FROM {{ ref('stg_active_permits') }}
WHERE application_date > CAST(
    '{{ var("as_of_date", run_started_at.strftime("%Y-%m-%d")) }}'
    AS DATE
)
AND source_valid_to IS NULL