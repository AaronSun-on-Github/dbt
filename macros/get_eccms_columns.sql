{% macro get_eccms_columns() %}

{{ return('CNT_BOTH_WEM_CONTACTTYPE,
            CNT_BOTH_WEM_ACCOUNTNUMBER,
            CNT_BOTH_WEM_SORTFIELD,
            CNT_BOTH_WEM_LASTNAME,
            CNT_BOTH_WEM_FIRSTNAME,
            CNT_BOTH_WEM_EFFECTIVEDATE,
            CNT_BOTH_WEM_EXPIRYDATE') }}

{% endmacro %}
-- eccms = 