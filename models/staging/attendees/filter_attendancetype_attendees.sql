with filter_attendancetype_attendees as
(
    select * 
    from {{ref('raw_attendees')}} t
    where t.ATTENDANCE_IND_ALL_ALL_ALL_ATTENDANCETYPE = 'Attended'
)

select * from filter_attendancetype_attendees