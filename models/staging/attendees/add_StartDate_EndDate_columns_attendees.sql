with add_StartDate_EndDate_columns_attendees as 

(
    select *, 
    ATTENDANCE_IND_ALL_ALL_ALL_STARTDATE as StartDate,
    ATTENDANCE_IND_ALL_ALL_ALL_ENDDATE as EndDate

    from {{ref('drop_duplicate_attendees')}}
)

select * from add_StartDate_EndDate_columns_attendees