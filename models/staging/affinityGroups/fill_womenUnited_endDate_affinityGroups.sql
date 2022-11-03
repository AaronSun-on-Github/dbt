with women_united_affinityGroups as
(
    select * 
    from {{ref('stg_affinityGroups_temp')}}
    where AFFIL_IND_AFFINITYGROUP_ACCOUNTNAME = 'Women United'
),

fill_UW_endDate as 
(
    select
    {{get_individual_columns()}},
    {{get_affinityGroups_columns()}}, 
    StartDate,
    coalesce(EndDate, date_format(current_date(),'yyyyMMdd')) as EndDate,
    StartDateLen,
    StartDate_to_date
    from women_united_affinityGroups

)

select * from fill_UW_endDate
