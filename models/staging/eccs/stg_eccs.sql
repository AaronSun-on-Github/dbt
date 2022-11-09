{% set column_names = get_individual_columns() + ',' + get_eccs_columns() %}

with drop_duplicate_eccs as 
(
    {{dbt_utils.deduplicate(
        relation=ref('raw_eccs'),
        partition_by=column_names,
        order_by= column_names
        )
    }}    
),

add_StartDate_EndDate_columns_eccs as 
(
    select *, 
    CNT_BOTH_ECC_EFFECTIVEDATE as StartDate,
    CNT_BOTH_ECC_EXPIRYDATE as EndDate
    from drop_duplicate_eccs
),

filter_null_StartDate_eccs as 
(
    select * 
    from add_StartDate_EndDate_columns_eccs
    where StartDate is not Null
),

filter_StartDate_length_eccs1 as 
(
    select *, 
    length(StartDate) as StartDateLen
    from filter_null_StartDate_eccs
),

filter_StartDate_length_eccs as 
(
    select *
    from filter_StartDate_length_eccs1
    where StartDateLen = 8
),

add_column_StartDate_to_date_eccs as 
(
    select *,
    try_cast({{parse_string_date_format('StartDate')}} as date)  as StartDate_to_date
    from filter_StartDate_length_eccs
),

fill_null_EndDate_eccs as
(
    select 
    INDACCOUNTNUMBER,
    INDFIRSTNAME,
    INDMIDDLENAME,
    INDLASTNAME,
    INDACCOUNTSTATUS,
    CNT_BOTH_ECC_CONTACTTYPE,
    CNT_BOTH_ECC_ACCOUNTNUMBER,
    CNT_BOTH_ECC_SORTFIELD,
    CNT_BOTH_ECC_LASTNAME,
    CNT_BOTH_ECC_FIRSTNAME,
    CNT_BOTH_ECC_EFFECTIVEDATE,
    CNT_BOTH_ECC_EXPIRYDATE,
    StartDate,
    coalesce(EndDate, StartDate) as EndDate,
    StartDateLen,
    StartDate_to_date
    from add_column_StartDate_to_date_eccs
),

filter_EndDate_length_eccs1 as 
(
    select *,
    length(EndDate) as EndDateLen
    from fill_null_EndDate_eccs
),

filter_EndDate_length_eccs as 
(
    select * 
    from filter_EndDate_length_eccs1
    where EndDateLen = 8
),

add_column_EndDate_to_date_eccs as 
(
    select *,
    try_cast({{parse_string_date_format('EndDate')}} as date)  as EndDate_to_date
    from filter_EndDate_length_eccs
),

filter_right_StartDate_EndDate_order_eccs1 as 
(
    select *,
    case when StartDate_to_date <= EndDate_to_date then 1 else 0 end as StartDate_EndDate_right_order
    from add_column_EndDate_to_date_eccs
),

filter_right_StartDate_EndDate_order_eccs as 
(
    select *
    from filter_right_StartDate_EndDate_order_eccs1
    where StartDate_EndDate_right_order = 1
),

drop_extra_column_eccs as 
(
    select 
    {{get_individual_columns()}},
    {{get_eccs_columns()}},
    StartDate_to_date,
    EndDate_to_date
    from filter_right_StartDate_EndDate_order_eccs
),

stg_eccs as 
(
    select *, 
    'Talent' as Level1, 
    'ECCs' as Level2, 
    t.CNT_BOTH_ECC_CONTACTTYPE as Level3,
    'ECCs' as Details
    from drop_extra_column_eccs t
)

select * from stg_eccs