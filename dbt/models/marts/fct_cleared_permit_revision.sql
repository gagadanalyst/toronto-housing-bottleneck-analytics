{{ config(materialized='table') }}
WITH current_source_rows AS (
    SELECT
        permit_num,
        revision_num,
        permit_type,
        builder_name,
        NULLIF(TRIM(status), '') AS status,
        application_date,
        issued_date,
        completed_date,
        geo_id,
    FROM {{ ref('stg_cleared_permits') }}
    WHERE source_valid_to IS NULL
),

revision_level AS (
    SELECT
        permit_num,
        revision_num,
        permit_type,
        COUNT(*) AS current_source_row_count,
        {{ ordered_distinct_string_agg('builder_name', "' | '", 'builder_name') }}
            AS observed_builder_names,
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
                COUNT(DISTINCT completed_date) > 1
                OR (
                    COUNT(completed_date) > 0
                    AND COUNT(completed_date) < COUNT(*)
                )
                THEN TRUE
            ELSE FALSE
        END AS completed_date_conflict,
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
                COUNT(completed_date) = COUNT(*)
                AND COUNT(DISTINCT completed_date) = 1
                THEN MIN(completed_date)
        END AS completed_date,
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
),

durations AS (
    SELECT
        *,
        {{ date_diff_days('application_date', 'issued_date') }}
            AS application_to_issue_days_raw,
        {{ date_diff_days('application_date', 'completed_date') }}
            AS application_to_completion_days_raw,
        {{ date_diff_days('issued_date', 'completed_date') }}
            AS issue_to_completion_days_raw
    FROM revision_level
)

SELECT
    *,
    CASE
        WHEN application_to_issue_days_raw >= 0
            THEN application_to_issue_days_raw
    END AS application_to_issue_days,
    CASE
        WHEN application_to_completion_days_raw >= 0
            THEN application_to_completion_days_raw
    END AS application_to_completion_days,
    CASE
        WHEN issue_to_completion_days_raw >= 0
            THEN issue_to_completion_days_raw
    END AS issue_to_completion_days,
    CASE
        WHEN application_to_issue_days_raw < 0 THEN TRUE
        WHEN application_to_issue_days_raw >= 0 THEN FALSE
    END AS application_to_issue_negative,
    CASE
        WHEN application_to_completion_days_raw < 0 THEN TRUE
        WHEN application_to_completion_days_raw >= 0 THEN FALSE
    END AS application_to_completion_negative,
    CASE
        WHEN issue_to_completion_days_raw < 0 THEN TRUE
        WHEN issue_to_completion_days_raw >= 0 THEN FALSE
    END AS issue_to_completion_negative
FROM durations
