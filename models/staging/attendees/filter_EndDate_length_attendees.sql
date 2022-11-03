with filter_EndDate_length_attendees1 as 
(
    select *,
    length(EndDate) as EndDateLen
    from {{ref('fill_null_EndDate_attendees')}}
),

filter_EndDate_length_attendees2 as 
(
    select * 
    from filter_EndDate_length_attendees1
    where EndDateLen = 8
)

select * from filter_EndDate_length_attendees2