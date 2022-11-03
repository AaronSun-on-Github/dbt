with add_column_StartDate_to_date_eccs as 
(
    select *,

    try_cast({{parse_string_date_format('StartDate')}} as date)  as StartDate_to_date

    from {{ref('filter_StartDate_length_eccs')}}
)

select * from add_column_StartDate_to_date_eccs