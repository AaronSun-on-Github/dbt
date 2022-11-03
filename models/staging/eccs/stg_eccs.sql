with drop_extra_column_eccs as 
(
    select 
    {{get_individual_columns()}},
    {{get_eccs_columns()}},
    StartDate_to_date,
    EndDate_to_date
    from {{ref('filter_right_StartDate_EndDate_order_eccs')}}
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