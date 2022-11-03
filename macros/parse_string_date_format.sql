{% macro parse_string_date_format(date_string)%}

-- left({{date_string}},4) + '-' + substr({{date_string}},5,2) + '-' + right({{date_string}},2)

    concat(left({{date_string}},4),'-',substr({{date_string}},5,2),'-',right({{date_string}},2))

{% endmacro %}