{% set table_name = 'affinityGroups' %}
{% set column_names = get_individual_columns() + ',' + get_affinityGroups_columns() %}

{{
    create_raw_element_table
    (
        table_name = table_name, 
        column_names = column_names
    )
}}

