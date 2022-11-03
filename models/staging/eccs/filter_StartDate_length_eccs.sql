with filter_StartDate_length_eccs1 as 
(
    select *, 

    length(StartDate) as StartDateLen

    from {{ref('filter_null_StartDate_eccs')}}
    

),

filter_StartDate_length_eccs2 as 
(
    select *

    from filter_StartDate_length_eccs1

    where StartDateLen = 8
)


select * from filter_StartDate_length_eccs2