with filter_EndDate_length_eccs1 as 
(
    select *,
    length(EndDate) as EndDateLen
    from {{ref('fill_null_EndDate_eccs')}}
),

filter_EndDate_length_eccs2 as 
(
    select * 
    from filter_EndDate_length_eccs1
    where EndDateLen = 8
)

select * from filter_EndDate_length_eccs2