WITH source_count AS (
    SELECT COUNT(*) AS row_count
    FROM {{ source('toronto_raw', 'active_permits') }}
),
staging_count AS (
    SELECT COUNT(*) AS row_count
    FROM {{ ref('stg_active_permits') }}
)
SELECT
    source_count.row_count AS source_rows,
    staging_count.row_count AS staging_rows
FROM source_count
CROSS JOIN staging_count
WHERE source_count.row_count <> staging_count.row_count