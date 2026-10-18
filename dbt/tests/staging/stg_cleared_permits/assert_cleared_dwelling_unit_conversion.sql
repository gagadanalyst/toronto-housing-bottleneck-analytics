SELECT
    source_uniq_id,
    'dwelling_units_created' AS field_name,
    dwelling_units_created_raw AS raw_value
FROM {{ ref('stg_cleared_permits') }}
WHERE
    dwelling_units_created_raw IS NOT NULL
    AND TRIM(dwelling_units_created_raw) <> ''
    AND dwelling_units_created IS NULL
UNION ALL
SELECT
    source_uniq_id,
    'dwelling_units_lost' AS field_name,
    dwelling_units_lost_raw AS raw_value
FROM {{ ref('stg_cleared_permits') }}
WHERE
    dwelling_units_lost_raw IS NOT NULL
    AND TRIM(dwelling_units_lost_raw) <> ''
    AND dwelling_units_lost IS NULL
