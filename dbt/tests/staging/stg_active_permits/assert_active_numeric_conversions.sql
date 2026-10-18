WITH conversion_errors AS (
    SELECT
        source_row_version_id,
        'est_const_cost' AS field_name,
        est_const_cost_raw AS raw_value
    FROM {{ ref('stg_active_permits') }}
    WHERE (
        NULLIF(TRIM(est_const_cost_raw), '') IS NULL
        AND est_const_cost IS NOT NULL
    )
    OR (
        UPPER(TRIM(est_const_cost_raw)) =
            'DO NOT UPDATE OR DELETE THIS INFO FIELD'
        AND est_const_cost IS NOT NULL
    )
    OR (
        NULLIF(TRIM(est_const_cost_raw), '') IS NOT NULL
        AND UPPER(TRIM(est_const_cost_raw)) <>
            'DO NOT UPDATE OR DELETE THIS INFO FIELD'
        AND (
            TRY_CAST(
                REPLACE(TRIM(est_const_cost_raw), ',', '')
                AS DOUBLE
            ) IS NULL
            OR est_const_cost IS DISTINCT FROM
                TRY_CAST(
                    REPLACE(TRIM(est_const_cost_raw), ',', '')
                    AS DOUBLE
                )
        )
    )

    UNION ALL

    SELECT
        source_row_version_id,
        'dwelling_units_created' AS field_name,
        dwelling_units_created_raw AS raw_value
    FROM {{ ref('stg_active_permits') }}
    WHERE (
        NULLIF(TRIM(dwelling_units_created_raw), '') IS NULL
        AND dwelling_units_created IS NOT NULL
    )
    OR (
        NULLIF(TRIM(dwelling_units_created_raw), '') IS NOT NULL
        AND (
            TRY_CAST(TRIM(dwelling_units_created_raw) AS BIGINT) IS NULL
            OR dwelling_units_created IS DISTINCT FROM
                TRY_CAST(TRIM(dwelling_units_created_raw) AS BIGINT)
        )
    )

    UNION ALL

    SELECT
        source_row_version_id,
        'dwelling_units_lost' AS field_name,
        dwelling_units_lost_raw AS raw_value
    FROM {{ ref('stg_active_permits') }}
    WHERE (
        NULLIF(TRIM(dwelling_units_lost_raw), '') IS NULL
        AND dwelling_units_lost IS NOT NULL
    )
    OR (
        NULLIF(TRIM(dwelling_units_lost_raw), '') IS NOT NULL
        AND (
            TRY_CAST(TRIM(dwelling_units_lost_raw) AS BIGINT) IS NULL
            OR dwelling_units_lost IS DISTINCT FROM
                TRY_CAST(TRIM(dwelling_units_lost_raw) AS BIGINT)
        )
    )
)
SELECT *
FROM conversion_errors