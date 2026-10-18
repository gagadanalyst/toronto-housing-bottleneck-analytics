SELECT
    source_uniq_id,
    est_const_cost_raw,
    est_const_cost
FROM {{ ref('stg_cleared_permits') }}
WHERE
    est_const_cost_raw IS NOT NULL
    AND TRIM(est_const_cost_raw) <> ''
    AND est_const_cost IS NULL
    AND UPPER(TRIM(est_const_cost_raw)) <> 'DO NOT UPDATE OR DELETE THIS INFO FIELD'
