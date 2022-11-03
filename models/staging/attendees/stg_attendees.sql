with map_L3_to_L2_attendees as 
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
    from {{ref('drop_extra_column_attendees')}}
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

