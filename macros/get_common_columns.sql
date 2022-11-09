{% macro get_common_columns() %}

{{ return('INDACCOUNTNUMBER,
    Level1, 
    Level2, 
    StartDate_to_date,
    EndDate_to_date,
    Details') }}

{% endmacro %}
