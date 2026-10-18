{% macro ordered_distinct_string_agg(expression, delimiter, order_by) -%}
    {{ return(adapter.dispatch('ordered_distinct_string_agg')(expression, delimiter, order_by)) }}
{%- endmacro %}

{% macro duckdb__ordered_distinct_string_agg(expression, delimiter, order_by) -%}
    STRING_AGG(DISTINCT {{ expression }}, {{ delimiter }} ORDER BY {{ order_by }})
{%- endmacro %}

{% macro snowflake__ordered_distinct_string_agg(expression, delimiter, order_by) -%}
    LISTAGG(DISTINCT {{ expression }}, {{ delimiter }})
        WITHIN GROUP (ORDER BY {{ order_by }})
{%- endmacro %}