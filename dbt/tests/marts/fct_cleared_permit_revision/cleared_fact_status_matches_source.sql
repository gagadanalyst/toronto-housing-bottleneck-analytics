WITH current_source_rows AS (
    SELECT
        permit_num,
        revision_num,
        permit_type,
        NULLIF(TRIM(status), '') AS status
    FROM {{ ref('stg_cleared_permits') }}
    WHERE source_valid_to IS NULL
),

source_revision_level AS (
    SELECT
        permit_num,
        revision_num,
        permit_type,
        COUNT(*) AS expected_source_row_count,
        {{ ordered_distinct_string_agg("COALESCE(status, '<NULL>')", "' | '", "COALESCE(status, '<NULL>')") }}
            AS expected_observed_status_values,
        CASE
            WHEN
                COUNT(DISTINCT status) > 1
                OR (COUNT(status) > 0 AND COUNT(status) < COUNT(*))
                THEN TRUE
            ELSE FALSE
        END AS expected_has_status_conflict,
        CASE
            WHEN
                COUNT(status) = COUNT(*)
                AND COUNT(DISTINCT status) = 1
                THEN MIN(status)
        END AS expected_status
    FROM current_source_rows
    GROUP BY permit_num, revision_num, permit_type
)

SELECT
    COALESCE(f.permit_num, s.permit_num) AS permit_num,
    COALESCE(f.revision_num, s.revision_num) AS revision_num,
    COALESCE(f.permit_type, s.permit_type) AS permit_type
FROM source_revision_level AS s
FULL OUTER JOIN {{ ref('fct_cleared_permit_revision') }} AS f
    ON
        f.permit_num = s.permit_num
        AND f.revision_num = s.revision_num
        AND f.permit_type = s.permit_type
WHERE
    s.expected_source_row_count IS NULL
    OR f.current_source_row_count IS NULL
    OR f.current_source_row_count IS DISTINCT FROM s.expected_source_row_count
    OR f.status IS DISTINCT FROM s.expected_status
    OR f.observed_status_values IS DISTINCT FROM s.expected_observed_status_values
    OR f.has_status_conflict IS DISTINCT FROM s.expected_has_status_conflict