{% set table_name = 'attendees' %}
{% set column_names = get_individual_columns() + ',' + get_eventAttendees_columns() %}

{{
    create_raw_element_table
    (
        table_name = table_name, 
        column_names = column_names
    )
}}
