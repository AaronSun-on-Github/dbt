/* {% set column_names = get_individual_columns() + ',' + get_opportunities_columns() %}


with drop_duplicate_opportunities as  -- investigate why this returns 0 rows 
(
        {{dbt_utils.deduplicate(
        relation=ref('raw_opportunities'),
        partition_by = column_names,
        order_by = column_names
        )
        }} 

) */

with drop_duplicate_opportunities as 
(
  select distinct * from {{ref('raw_opportunities')}} 
),

add_StartDate_EndDate_column_opportunities as 
(
    select *, 
    VOLASSIGNED_IND_ANY_STARTDATE as StartDate,
    VOLASSIGNED_IND_ANY_ENDDATE as EndDate
    from drop_duplicate_opportunities
),

filter_null_StartDate_opportunities as 
(
    select * 

    from add_StartDate_EndDate_column_opportunities

    where StartDate is not Null
),

filter_StartDate_length_opportunities1 as 
(
    select *, 

    length(StartDate) as StartDateLen

    from filter_null_StartDate_opportunities

),

filter_StartDate_length_opportunities2 as 
(
    select *

    from filter_StartDate_length_opportunities1

    where StartDateLen = 8
),

add_column_StartDate_to_date_opportunities as 
(
    select *,

    try_cast({{parse_string_date_format('StartDate')}} as date)  as StartDate_to_date

    from filter_StartDate_length_opportunities2

),

fill_null_EndDate_opportunities as 
(
    select
    {{get_individual_columns()}},
    {{get_opportunities_columns()}}, 
    StartDate,
    coalesce(EndDate, StartDate) as EndDate,
    StartDateLen,
    StartDate_to_date
    from add_column_StartDate_to_date_opportunities

),

filter_EndDate_length_opportunities1 as 
(
    select *,
    length(EndDate) as EndDateLen
    from fill_null_EndDate_opportunities
),

filter_EndDate_length_opportunities2 as 
(
    select * 
    from filter_EndDate_length_opportunities1
    where EndDateLen = 8
),

add_column_EndDate_to_date_opportunities as 
(
    select *,

    try_cast({{parse_string_date_format('EndDate')}} as date)  as EndDate_to_date

    from filter_EndDate_length_opportunities2
),

filter_right_StartDate_EndDate_order_opportunities1 as 
(
    select *,
    case when StartDate_to_date <= EndDate_to_date then 1 else 0 end as StartDate_EndDate_right_order
    from add_column_EndDate_to_date_opportunities
),

filter_right_StartDate_EndDate_order_opportunities2 as 
(
    select *
    from filter_right_StartDate_EndDate_order_opportunities1
    where StartDate_EndDate_right_order = 1
),

drop_extra_column_opportunities as 
(
    select 
    {{get_individual_columns()}},
    {{get_opportunities_columns()}},
    StartDate_to_date,
    EndDate_to_date
    from filter_right_StartDate_EndDate_order_opportunities2
),

map_opsclass_to_L2 as 
( 
    select *, 
    case 
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Board' then	'Board of Directors'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Cabinet' then	'Cabinet Member'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Community Stewardship Committee' then	'Community Stewardship Committee'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Event' then	'Event Attendee'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'General' then	'Volunteer Opportunity'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Speaker' then	'Speaker Series'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Speaker Series Volunteer' then	'Speaker Series'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Training' then	'Training Session'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Days of Caring' then	'Days of Caring'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Poverty Simulation' then	'Poverty Simulation'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Project' then	'Project Participant'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Meeting' then	'Meeting Attendee'
        else null
    end as Level2
    from drop_extra_column_opportunities
),

map_opsclass_to_L1 as 
(
    select *, 
    case
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Board' then	'Talent'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Cabinet' then	'Talent'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Community Stewardship Committee' then	'Talent'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Event' then	'Talent'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'General' then	'Talent'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Speaker' then	'Talent'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Speaker Series Volunteer' then	'Talent'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Training' then	'Time'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Days of Caring' then	'Time'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Poverty Simulation' then	'Time'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Project' then	'Talent'
        when VOLASSIGNED_IND_ANY_OPSCLASS = 'Meeting' then	'Talent'
        else null
    end as Level1,
    VOLASSIGNED_IND_ANY_JOBTITLE as Details
    from map_opsclass_to_L2
)

select * from map_opsclass_to_L1

