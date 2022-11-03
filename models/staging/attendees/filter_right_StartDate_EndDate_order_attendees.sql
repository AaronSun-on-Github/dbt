with filter_right_StartDate_EndDate_order_attendees_temp1 as 
(
    select *,
    case when StartDate_to_date <= EndDate_to_date then 1 else 0 end as StartDate_EndDate_right_order

    from {{ref('add_column_EndDate_to_date_attendees')}}
),

filter_right_StartDate_EndDate_order_attendees as 
(
    select *
    from filter_right_StartDate_EndDate_order_attendees_temp1
    where StartDate_EndDate_right_order = 1
)

select * from filter_right_StartDate_EndDate_order_attendees