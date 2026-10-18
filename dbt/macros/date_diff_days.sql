{% macro date_diff_days(start_date, end_date) -%}
    {{ return(adapter.dispatch('date_diff_days')(start_date, end_date)) }}
{%- endmacro %}

{% macro duckdb__date_diff_days(start_date, end_date) -%}
    date_diff('day', {{ start_date }}, {{ end_date }})
{%- endmacro %}

{% macro snowflake__date_diff_days(start_date, end_date) -%}
    datediff(day, {{ start_date }}, {{ end_date }})
{%- endmacro %}