{% set table_name = 'org_treasure' %}
{% set column_names = get_organization_columns() + ',' + get_treasure_columns() %}

{{
    create_raw_element_table
    (
        table_name = table_name, 
        column_names = column_names
    )
}} 