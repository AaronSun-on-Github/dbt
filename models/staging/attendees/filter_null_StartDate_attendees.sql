with filter_null_StartDate_attendees as 
(
    select * 

    from {{ref('add_StartDate_EndDate_columns_attendees')}}

    where StartDate is not Null
)


select * from filter_null_StartDate_attendees
-- select count(*) from filter_null_StartDate_attendees
