with add_column_EndDate_to_date_attendees as 
(
    select *,

    try_cast({{parse_string_date_format('EndDate')}} as date)  as EndDate_to_date

    from {{ref('filter_EndDate_length_attendees')}}
)

select * from add_column_EndDate_to_date_attendees

