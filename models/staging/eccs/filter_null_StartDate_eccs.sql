with filter_null_StartDate_eccs as 
(
    select * 

    from {{ref('add_StartDate_EndDate_columns_eccs')}}

    where StartDate is not Null
)


select * from filter_null_StartDate_eccs
-- select count(*) from filter_null_StartDate
