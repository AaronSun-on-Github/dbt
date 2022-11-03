
{% set column_names = get_individual_columns() + ',' + get_affinityGroups_columns() %}

with drop_duplicate_affinityGroups as 
(
        {{dbt_utils.deduplicate(
        relation=ref('raw_affinityGroups'),
        partition_by = column_names,
        order_by = column_names
        )
        }} 
),


add_StartDate_EndDate_column_affinityGroups as 
(
    select *, 
    AFFIL_IND_AFFINITYGROUP_EFFECTIVEDATE as StartDate,
    AFFIL_IND_AFFINITYGROUP_EXPIRYDATE as EndDate
    from drop_duplicate_affinityGroups
),

filter_null_StartDate_affinityGroups as 
(
    select * 

    from add_StartDate_EndDate_column_affinityGroups

    where StartDate is not Null
),

filter_StartDate_length_affinityGroups1 as 
(
    select *, 

    length(StartDate) as StartDateLen

    from filter_null_StartDate_affinityGroups

),

filter_StartDate_length_affinityGroups2 as 
(
    select *

    from filter_StartDate_length_affinityGroups1

    where StartDateLen = 8
),

add_column_StartDate_to_date_affinityGroups as 
(
    select *,

    try_cast({{parse_string_date_format('StartDate')}} as date)  as StartDate_to_date

    from filter_StartDate_length_affinityGroups2

)

select * from add_column_StartDate_to_date_affinityGroups










