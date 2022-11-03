{% set table_name = 'eccms' %}
{% set column_names = get_individual_columns() + ',' + get_eccms_columns() %}

{{
    create_raw_element_table
    (
        table_name = table_name, 
        column_names = column_names
    )
}}

