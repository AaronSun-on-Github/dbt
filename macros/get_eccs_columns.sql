{% macro get_eccs_columns() %}

{{ return('CNT_BOTH_ECC_CONTACTTYPE,
            CNT_BOTH_ECC_ACCOUNTNUMBER,
            CNT_BOTH_ECC_SORTFIELD,
            CNT_BOTH_ECC_LASTNAME,
            CNT_BOTH_ECC_FIRSTNAME,
            CNT_BOTH_ECC_EFFECTIVEDATE,
            CNT_BOTH_ECC_EXPIRYDATE') }}

{% endmacro %}
