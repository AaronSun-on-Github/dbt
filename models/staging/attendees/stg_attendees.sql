{% set column_names = get_individual_columns() + ',' + get_eventAttendees_columns() %}

with filter_attendancetype_attendees as
(
    select * 
    from {{ref('raw_attendees')}} t
    where t.ATTENDANCE_IND_ALL_ALL_ALL_ATTENDANCETYPE = 'Attended'
),

drop_duplicate_attendees as 
(
    {{dbt_utils.deduplicate(
        relation = 'filter_attendancetype_attendees',
        partition_by = column_names,
        order_by = column_names
        )
    }}    
),

add_StartDate_EndDate_columns_attendees as 
(
    select *, 
    ATTENDANCE_IND_ALL_ALL_ALL_STARTDATE as StartDate,
    ATTENDANCE_IND_ALL_ALL_ALL_ENDDATE as EndDate
    from drop_duplicate_attendees
),

filter_null_StartDate_attendees as 
(
    select * 
    from add_StartDate_EndDate_columns_attendees
    where StartDate is not Null
),

filter_StartDate_length_attendees1 as 
(
    select *, 
    length(StartDate) as StartDateLen
    from filter_null_StartDate_attendees
),

filter_StartDate_length_attendees as 
(
    select *
    from filter_StartDate_length_attendees1
    where StartDateLen = 8
),

add_column_StartDate_to_date_attendees as 
(
    select *,
    try_cast({{parse_string_date_format('StartDate')}} as date)  as StartDate_to_date
    from filter_StartDate_length_attendees
),

fill_null_EndDate_attendees as
(
    select 
    INDACCOUNTNUMBER,
    INDFIRSTNAME,
    INDMIDDLENAME,
    INDLASTNAME,
    INDACCOUNTSTATUS,
    ATTENDANCE_IND_ALL_ALL_ALL_EVENTACCOUNT,
    ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME,
    ATTENDANCE_IND_ALL_ALL_ALL_OCCURRENCE,
    ATTENDANCE_IND_ALL_ALL_ALL_STARTDATE,
    ATTENDANCE_IND_ALL_ALL_ALL_STARTTIME,
    ATTENDANCE_IND_ALL_ALL_ALL_ENDDATE,
    ATTENDANCE_IND_ALL_ALL_ALL_ENDTIME,
    ATTENDANCE_IND_ALL_ALL_ALL_EVENTCLASS,
    ATTENDANCE_IND_ALL_ALL_ALL_WEBDESCRIPTION,
    ATTENDANCE_IND_ALL_ALL_ALL_ATTENDANCETYPE,
    StartDate,
    coalesce(EndDate, StartDate) as EndDate,
    StartDateLen,
    StartDate_to_date

    from add_column_StartDate_to_date_attendees
),

filter_EndDate_length_attendees1 as 
(
    select *,
    length(EndDate) as EndDateLen
    from fill_null_EndDate_attendees
),

filter_EndDate_length_attendees as 
(
    select * 
    from filter_EndDate_length_attendees1
    where EndDateLen = 8
),

add_column_EndDate_to_date_attendees as 
(
    select *,
    try_cast({{parse_string_date_format('EndDate')}} as date)  as EndDate_to_date
    from filter_EndDate_length_attendees
),

filter_right_StartDate_EndDate_order_attendees_temp1 as 
(
    select *,
    case when StartDate_to_date <= EndDate_to_date then 1 else 0 end as StartDate_EndDate_right_order
    from add_column_EndDate_to_date_attendees
),

filter_right_StartDate_EndDate_order_attendees as 
(
    select *
    from filter_right_StartDate_EndDate_order_attendees_temp1
    where StartDate_EndDate_right_order = 1
),

drop_extra_column_attendees as 
(
    select 
    {{get_individual_columns()}},
    {{get_eventAttendees_columns()}},
    StartDate_to_date,
    EndDate_to_date
    from filter_right_StartDate_EndDate_order_attendees
),

map_L3_to_L2_attendees as 
(
    select *,
    ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME as Level3,
    case 
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Virtual Tours' then  'AIFY'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Ambassador Training' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Campaign Training' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Collection Drives' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Everyday Hero Event Series' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'GenNext Activities' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'KCI Presentation - Getting ready for the' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Leadership Gifts Cabinet Meetings' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Red Feather' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Red Tie Gala' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Speaker Training' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'United Way Speaker Series' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Women United Cabinet Meetings' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Workplace Campaigns Cabinet Meetings' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Brain Game' then 	'Brain Game'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Collection Drives' then 	'Coats for Kids & Families'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'GenNext Activities' then 	'Coats for Kids & Families'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Community Impact Tours' then 	'Community Programs'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Engineering Challenge Activities' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'GenNext Activities' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Leadership Gifts Cabinet Meetings' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Collection Drives' then 	'Tools for School'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Collection Drives' then 	'Tools for School'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Community Events' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Ruth Kelly Events' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Tomorrow Fund Events' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Pillars of Change' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Poverty Simulation' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'United Way Sharing Caf�' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'YEG Downtown Collaborative Activities' then	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Women United Stewardship Events' then 	'Event Attendee'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Heartland Challenge Activities' then 	'Event Attendee'
        else null
    end as Level2
    from drop_extra_column_attendees
),

map_L3_to_L1_attendees as 
(
    select *,
    case 
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Virtual Tours' then 'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Ambassador Training' then	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Campaign Training' then 'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Collection Drives' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Everyday Hero Event Series' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'GenNext Activities' then 	'Talent'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'KCI Presentation - Getting ready for the' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Leadership Gifts Cabinet Meetings' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Red Feather' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Red Tie Gala' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Speaker Training' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'United Way Speaker Series' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Women United Cabinet Meetings' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Workplace Campaigns Cabinet Meetings' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Brain Game' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Collection Drives' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'GenNext Activities' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Community Impact Tours' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Engineering Challenge Activities' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'GenNext Activities' then 	'Talent'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Leadership Gifts Cabinet Meetings' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Collection Drives' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Collection Drives' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Community Events' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Ruth Kelly Events' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Tomorrow Fund Events' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Pillars of Change' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Poverty Simulation' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'United Way Sharing Caf�' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'YEG Downtown Collaborative Activities' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Women United Stewardship Events' then 	'Time'
        when ATTENDANCE_IND_ALL_ALL_ALL_EVENTNAME = 'Heartland Challenge Activities' then	'Time'
        else null
    end as Level1,
    ATTENDANCE_IND_ALL_ALL_ALL_WEBDESCRIPTION as Details
    from map_L3_to_L2_attendees
)

select * from map_L3_to_L1_attendees

