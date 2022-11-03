with drop_extra_column_attendees as 
(
    select 
    {{get_individual_columns()}},
    {{get_eventAttendees_columns()}},
    StartDate_to_date,
    EndDate_to_date
    from {{ref('filter_right_StartDate_EndDate_order_attendees')}}
)

select * from drop_extra_column_attendees