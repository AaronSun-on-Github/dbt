with unioned_affinityGroups as 
(
    select * 
    from {{ref('fill_womenUnited_endDate_affinityGroups')}}
    union
    select *
    from {{ref('fill_scr_endDate_affinityGroups')}}
),

filter_EndDate_length_affinityGroups1 as 
(
    select *,
    length(EndDate) as EndDateLen
    from unioned_affinityGroups
),

filter_EndDate_length_affinityGroups2 as 
(
    select * 
    from filter_EndDate_length_affinityGroups1
    where EndDateLen = 8
),

add_column_EndDate_to_date_affinityGroups as 
(
    select *,

    try_cast({{parse_string_date_format('EndDate')}} as date)  as EndDate_to_date

    from filter_EndDate_length_affinityGroups2
),

filter_right_StartDate_EndDate_order_affinityGroups1 as 
(
    select *,
    case when StartDate_to_date <= EndDate_to_date then 1 else 0 end as StartDate_EndDate_right_order
    from add_column_EndDate_to_date_affinityGroups
),

filter_right_StartDate_EndDate_order_affinityGroups2 as 
(
    select *
    from filter_right_StartDate_EndDate_order_affinityGroups1
    where StartDate_EndDate_right_order = 1
),

drop_extra_column_affinityGroups as 
(
    select 
    {{get_individual_columns()}},
    {{get_affinityGroups_columns()}},
    StartDate_to_date,
    EndDate_to_date
    from filter_right_StartDate_EndDate_order_affinityGroups2
),

stg_affinityGroups as 
(
        select *,
        'Time' as Level1,
        'Affinity Groups' as Level2, 
        AFFIL_IND_AFFINITYGROUP_ACCOUNTNAME as Level3,
        'Affinity Groups' as Details
        from drop_extra_column_affinityGroups
)

select * from stg_affinityGroups

