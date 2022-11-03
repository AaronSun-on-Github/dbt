{% macro get_agencySpeakers_columns() %}

{{ return('AFFIL_IND_AGENCYSPKR_ACCOUNTNUMBER,
            AFFIL_IND_AGENCYSPKR_ACCOUNTNAME,
            AFFIL_IND_AGENCYSPKR_EFFECTIVEDATE,
            AFFIL_IND_AGENCYSPKR_EXPIRYDATE') }}

{% endmacro %}
-- agencySpeakers = 