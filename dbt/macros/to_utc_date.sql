{% macro to_utc_date(expression) -%}
    {{ return(adapter.dispatch('to_utc_date')(expression)) }}
{%- endmacro %}

{% macro duckdb__to_utc_date(expression) -%}
    CAST(timezone('UTC', {{ expression }}) AS DATE)
{%- endmacro %}

{% macro snowflake__to_utc_date(expression) -%}
    CAST(CONVERT_TIMEZONE('UTC', {{ expression }}) AS DATE)
{%- endmacro %}