{% macro create_raw_element_table(table_name, column_names ) %}

with raw_{{table_name}} as
(
    select

    {{column_names}}

    from {{source('src_ttt','raw_ttt')}}

)

select * from raw_{{table_name}}

{% endmacro %}