
{% set column_names = get_individual_columns() + ',' + get_eccms_columns() %}

with drop_duplicate_eccms as 
(
        {{dbt_utils.deduplicate(
        relation=ref('raw_eccms'),
        partition_by = column_names,
        order_by = column_names
        )
        }} 
),


add_StartDate_EndDate_column_eccms as 
(
    select *, 
    CNT_BOTH_WEM_EFFECTIVEDATE as StartDate,
    CNT_BOTH_WEM_EXPIRYDATE as EndDate
    from drop_duplicate_eccms
),

filter_null_StartDate_eccms as 
(
    select * 

    from add_StartDate_EndDate_column_eccms

    where StartDate is not Null
),

filter_StartDate_length_eccms1 as 
(
    select *, 

    length(StartDate) as StartDateLen

    from filter_null_StartDate_eccms

),

filter_StartDate_length_eccms2 as 
(
    select *

    from filter_StartDate_length_eccms1

    where StartDateLen = 8
),

add_column_StartDate_to_date_eccms as 
(
    select *,

    try_cast({{parse_string_date_format('StartDate')}} as date)  as StartDate_to_date

    from filter_StartDate_length_eccms2

),

fill_null_EndDate_eccms as 
(
    select
    {{get_individual_columns()}},
    {{get_eccms_columns()}}, 
    StartDate,
    coalesce(EndDate, StartDate) as EndDate,
    StartDateLen,
    StartDate_to_date
    from add_column_StartDate_to_date_eccms

),

filter_EndDate_length_eccms1 as 
(
    select *,
    length(EndDate) as EndDateLen
    from fill_null_EndDate_eccms
),

filter_EndDate_length_eccms2 as 
(
    select * 
    from filter_EndDate_length_eccms1
    where EndDateLen = 8
),

add_column_EndDate_to_date_eccms as 
(
    select *,

    try_cast({{parse_string_date_format('EndDate')}} as date)  as EndDate_to_date

    from filter_EndDate_length_eccms2
),

filter_right_StartDate_EndDate_order_eccms1 as 
(
    select *,
    case when StartDate_to_date <= EndDate_to_date then 1 else 0 end as StartDate_EndDate_right_order
    from add_column_EndDate_to_date_eccms
),

filter_right_StartDate_EndDate_order_eccms2 as 
(
    select *
    from filter_right_StartDate_EndDate_order_eccms1
    where StartDate_EndDate_right_order = 1
),

drop_extra_column_eccms as 
(
    select 
    {{get_individual_columns()}},
    {{get_eccms_columns()}},
    StartDate_to_date,
    EndDate_to_date
    from filter_right_StartDate_EndDate_order_eccms2
),

stg_eccms as 
(
        select *,
        'Talent' as Level1,
        'ECCMs' as Level2, 
        CNT_BOTH_WEM_CONTACTTYPE as Level3,
        'ECCMs' as Details
        from drop_extra_column_eccms
)

select * from stg_eccms

