
{% set table_name = 'opportunities' %}
{% set column_names = get_individual_columns() + ',' + get_opportunities_columns() %}

{{
    create_raw_element_table
    (
        table_name = table_name, 
        column_names = column_names
    )
}}

