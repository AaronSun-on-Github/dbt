{% set table_name = 'ind_treasure' %}
{% set column_names = get_individual_columns() + ',' + get_treasure_columns() %}

{{
    create_raw_element_table
    (
        table_name = table_name, 
        column_names = column_names
    )
}}