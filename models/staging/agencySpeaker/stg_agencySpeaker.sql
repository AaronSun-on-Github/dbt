with filter_null_StartDate_agencySpeaker as 
(
    select * 

    from {{ref('add_StartDate_EndDate_column_agencySpeaker')}}

    where StartDate is not Null
),

filter_StartDate_length_agencySpeaker1 as 
(
    select *, 

    length(StartDate) as StartDateLen

    from filter_null_StartDate_agencySpeaker

),

filter_StartDate_length_agencySpeaker2 as 
(
    select *

    from filter_StartDate_length_agencySpeaker1

    where StartDateLen = 8
),

add_column_StartDate_to_date_agencySpeaker as 
(
    select *,

    try_cast({{parse_string_date_format('StartDate')}} as date)  as StartDate_to_date

    from filter_StartDate_length_agencySpeaker2

),

fill_null_EndDate_agencySpeaker as 
(
    select
    {{get_individual_columns()}},
    {{get_agencySpeakers_columns()}}, 
    StartDate,
    coalesce(EndDate, StartDate) as EndDate,
    StartDateLen,
    StartDate_to_date
    from add_column_StartDate_to_date_agencySpeaker

),

filter_EndDate_length_agencySpeaker1 as 
(
    select *,
    length(EndDate) as EndDateLen
    from fill_null_EndDate_agencySpeaker
),

filter_EndDate_length_agencySpeaker2 as 
(
    select * 
    from filter_EndDate_length_agencySpeaker1
    where EndDateLen = 8
),

add_column_EndDate_to_date_agencySpeaker as 
(
    select *,

    try_cast({{parse_string_date_format('EndDate')}} as date)  as EndDate_to_date

    from filter_EndDate_length_agencySpeaker2
),

filter_right_StartDate_EndDate_order_agencySpeaker1 as 
(
    select *,
    case when StartDate_to_date <= EndDate_to_date then 1 else 0 end as StartDate_EndDate_right_order
    from add_column_EndDate_to_date_agencySpeaker
),

filter_right_StartDate_EndDate_order_agencySpeaker2 as 
(
    select *
    from filter_right_StartDate_EndDate_order_agencySpeaker1
    where StartDate_EndDate_right_order = 1
),


drop_extra_column_agencySpeaker as 
(
    select 
    {{get_individual_columns()}},
    {{get_agencySpeakers_columns()}},
    StartDate_to_date,
    EndDate_to_date
    from filter_right_StartDate_EndDate_order_agencySpeaker2
),

stg_agencySpeaker as 
(
    select *, 
    'Talent' as Level1,
    'Agency Speaker' as Level2,
    t.AFFIL_IND_AGENCYSPKR_ACCOUNTNAME as Details
    from drop_extra_column_agencySpeaker t
)

select * from stg_agencySpeaker

