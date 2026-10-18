WITH raw_count AS (
    SELECT COUNT(*) AS row_count
    FROM {{ source('toronto_raw', 'cleared_permits') }}
),
staging_count AS (
    SELECT COUNT(*) AS row_count
    FROM {{ ref('stg_cleared_permits') }}
)
SELECT
    raw_count.row_count AS raw_rows,
    staging_count.row_count AS staging_rows
FROM raw_count
CROSS JOIN staging_count
WHERE raw_count.row_count <> staging_count.row_count