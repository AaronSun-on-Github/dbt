{% macro get_affinityGroups_columns() %}

{{ return('AFFIL_IND_AFFINITYGROUP_ACCOUNTNUMBER,
            AFFIL_IND_AFFINITYGROUP_ACCOUNTNAME,
            AFFIL_IND_AFFINITYGROUP_EFFECTIVEDATE,
            AFFIL_IND_AFFINITYGROUP_EXPIRYDATE') }}

{% endmacro %}
-- affinityGroups = 