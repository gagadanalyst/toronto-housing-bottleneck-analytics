WITH current_source_rows AS (
    SELECT
        permit_num,
        revision_num,
        permit_type,
        NULLIF(TRIM(builder_name), '') AS builder_name,
        NULLIF(TRIM(status), '') AS status,
        application_date,
        issued_date,
        geo_id
    FROM {{ ref('stg_active_permits') }}
    WHERE source_valid_to IS NULL
),

permit_revision_counts AS (
    -- Group by the three-column business key.
    -- Count physical source rows for each logical revision.
    SELECT
        permit_num,
        revision_num,
        permit_type,
        COUNT(*) AS current_source_row_count,
        {{ ordered_distinct_string_agg("COALESCE(builder_name, '<NULL>')", "' | '", "COALESCE(builder_name, '<NULL>')") }}
            AS observed_builder_names,
        CASE
            WHEN
                COUNT(builder_name) = COUNT(*)
                AND COUNT(DISTINCT builder_name) = 1
                THEN MIN(builder_name)
        END AS builder_name,
        CASE
            WHEN
                COUNT(DISTINCT builder_name) > 1
                OR (
                    COUNT(builder_name) > 0
                    AND COUNT(builder_name) < COUNT(*)
                )
                THEN TRUE
            ELSE FALSE
        END AS builder_name_conflict,
        {{ ordered_distinct_string_agg("COALESCE(status, '<NULL>')", "' | '", "COALESCE(status, '<NULL>')") }}
            AS observed_status_values,
        CASE
            WHEN
                COUNT(DISTINCT status) > 1
                OR (
                    COUNT(status) > 0
                    AND COUNT(status) < COUNT(*)
                )
                THEN TRUE
            ELSE FALSE
        END AS has_status_conflict,
        {{ ordered_distinct_string_agg("COALESCE(CAST(application_date AS VARCHAR), '<NULL>')", "' | '", "COALESCE(CAST(application_date AS VARCHAR), '<NULL>')") }}
            AS observed_application_dates,

        {{ ordered_distinct_string_agg("COALESCE(CAST(issued_date AS VARCHAR), '<NULL>')", "' | '", "COALESCE(CAST(issued_date AS VARCHAR), '<NULL>')") }}
            AS observed_issued_dates,

        {{ ordered_distinct_string_agg("COALESCE(CAST(geo_id AS VARCHAR), '<NULL>')", "' | '", "COALESCE(CAST(geo_id AS VARCHAR), '<NULL>')") }}
            AS observed_geo_ids,
        CASE
            WHEN
                COUNT(status) = COUNT(*)
                AND COUNT(DISTINCT status) = 1
                THEN MIN(status)
        END AS status,
        CASE
            WHEN
                COUNT(DISTINCT application_date) > 1
                OR (
                    COUNT(application_date) > 0
                    AND COUNT(application_date) < COUNT(*)
                )
                THEN TRUE
            ELSE FALSE
        END AS application_date_conflict,
        CASE
            WHEN
                COUNT(DISTINCT issued_date) > 1
                OR (
                    COUNT(issued_date) > 0
                    AND COUNT(issued_date) < COUNT(*)
                )
                THEN TRUE
            ELSE FALSE
        END AS issued_date_conflict,
        CASE
            WHEN
                COUNT(DISTINCT geo_id) > 1
                OR (
                    COUNT(geo_id) > 0
                    AND COUNT(geo_id) < COUNT(*)
                )
                THEN TRUE
            ELSE FALSE
        END AS geo_id_conflict,
        CASE
            WHEN
                COUNT(application_date) = COUNT(*)
                AND COUNT(DISTINCT application_date) = 1
                THEN MIN(application_date)
        END AS application_date,
        CASE
            WHEN
                COUNT(issued_date) = COUNT(*)
                AND COUNT(DISTINCT issued_date) = 1
                THEN MIN(issued_date)
        END AS issued_date,
        CASE
            WHEN
                COUNT(geo_id) = COUNT(*)
                AND COUNT(DISTINCT geo_id) = 1
                THEN MIN(geo_id)
        END AS geo_id
    FROM current_source_rows
    GROUP BY
        permit_num,
        revision_num,
        permit_type
)

-- Return one row per logical permit revision.
select * from permit_revision_counts
