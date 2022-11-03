with SCR_affinityGroups as
(
    select * 
    from {{ref('stg_affinityGroups_temp')}}
    where AFFIL_IND_AFFINITYGROUP_ACCOUNTNAME != 'Women United'
),

fill_scr_EndDate as 
(
    select
    {{get_individual_columns()}},
    {{get_affinityGroups_columns()}}, 
    StartDate,
    coalesce(EndDate, StartDate) as EndDate,
    StartDateLen,
    StartDate_to_date
    from SCR_affinityGroups

)

select * from fill_scr_EndDate
