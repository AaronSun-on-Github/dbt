
with filter_StartDate_length_attendees1 as 
(
    select *, 

    length(StartDate) as StartDateLen

    from {{ref('filter_null_StartDate_attendees')}}
    

),

filter_StartDate_length_attendees2 as 
(
    select *

    from filter_StartDate_length_attendees1

    where StartDateLen = 8
)


select * from filter_StartDate_length_attendees2