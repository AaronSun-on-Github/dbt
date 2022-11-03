{% set column_names = get_individual_columns() + ',' + get_eventAttendees_columns() %}


with drop_duplicate_attendees as 
(
        {{dbt_utils.deduplicate(
          relation=ref('filter_attendancetype_attendees'),
          partition_by= column_names,
          order_by= column_names
          )
        }}    
)

select * from drop_duplicate_attendees