with filter_right_StartDate_EndDate_order_eccs1 as 
(
    select *,
    case when StartDate_to_date <= EndDate_to_date then 1 else 0 end as StartDate_EndDate_right_order

    from {{ref('add_column_EndDate_to_date_eccs')}}
),

filter_right_StartDate_EndDate_order_eccs2 as 
(
    select *
    from filter_right_StartDate_EndDate_order_eccs1
    where StartDate_EndDate_right_order = 1
)

select * from filter_right_StartDate_EndDate_order_eccs2