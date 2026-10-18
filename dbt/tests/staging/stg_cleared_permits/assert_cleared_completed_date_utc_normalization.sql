SELECT
    source_uniq_id,
    completed_date_raw,
    completed_date,
    {{ to_utc_date('completed_date_raw') }} AS expected_completed_date
FROM {{ ref('stg_cleared_permits') }}
WHERE completed_date IS DISTINCT FROM {{ to_utc_date('completed_date_raw') }}
