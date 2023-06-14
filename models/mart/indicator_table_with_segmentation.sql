with 
Apr_TY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2022-05-01'),
May_TY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2022-06-01'),
Jun_TY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2022-07-01'),
Jul_TY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2022-08-01'),
Aug_TY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2022-09-01'),
Sep_TY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2022-10-01'),
Oct_TY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2022-11-01'),
Nov_TY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2022-12-01'),
Dec_TY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2023-01-01'),
Jan_TY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2023-02-01'),
Feb_TY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2023-03-01'),
Mar_TY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2023-04-01'),

Apr_LY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2021-05-01'),
May_LY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2021-06-01'),
Jun_LY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2021-07-01'),
Jul_LY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2021-08-01'),
Aug_LY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2021-09-01'),
Sep_LY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2021-10-01'),
Oct_LY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2021-11-01'),
Nov_LY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2021-12-01'),
Dec_LY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2022-01-01'),
Jan_LY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2022-02-01'),
Feb_LY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2022-03-01'),
Mar_LY as (select * from {{ref('fact_time_talent_treasure')}} where log <'2022-04-01'),

{% set TY_months = get_TY_month() %}
{% for TY_month in TY_months %}
{{TY_month}}_Donation as (
    select distinct ENTITYACCOUNTNUMBER,
    max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_{{TY_month}}
    from {{TY_month}}
    group by ENTITYACCOUNTNUMBER
    order by ENTITYACCOUNTNUMBER
),
{% endfor %}


{% set LY_months = get_LY_month() %}
{%for LY_month in LY_months%}
{{LY_month}}_Donation as (
    select distinct ENTITYACCOUNTNUMBER,
    max(case when CampaignYear = 2021 then 1 else 0 end) as d_in_{{LY_month}}
    from {{LY_month}}
    group by ENTITYACCOUNTNUMBER
    order by ENTITYACCOUNTNUMBER
),
{% endfor %} 

-- Apr_TY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Apr_TY
-- from Apr_TY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- May_TY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_May_TY
-- from May_TY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Jun_TY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Jun_TY
-- from Jun_TY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Jul_TY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Jul_TY
-- from Jul_TY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Aug_TY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Aug_TY
-- from Aug_TY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Sep_TY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Sep_TY
-- from Sep_TY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Oct_TY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Oct_TY
-- from Oct_TY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Nov_TY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Nov_TY
-- from Nov_TY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Dec_TY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Dec_TY
-- from Dec_TY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Jan_TY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Jan_TY
-- from Jan_TY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Feb_TY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Feb_TY
-- from Feb_TY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Mar_TY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Mar_TY
-- from Mar_TY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),


-- Apr_LY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Apr_LY
-- from Apr_LY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- May_LY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_May_LY
-- from May_LY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Jun_LY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Jun_LY
-- from Jun_LY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Jul_LY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Jul_LY
-- from Jul_LY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Aug_LY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Aug_LY
-- from Aug_LY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Sep_LY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Sep_LY
-- from Sep_LY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Oct_LY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Oct_LY
-- from Oct_LY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Nov_LY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Nov_LY
-- from Nov_LY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Dec_LY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Dec_LY
-- from Dec_LY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Jan_LY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Jan_LY
-- from Jan_LY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Feb_LY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Feb_LY
-- from Feb_LY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),

-- Mar_LY_Donation as (
-- select distinct ENTITYACCOUNTNUMBER, 
-- max(case when CampaignYear = 2022 then 1 else 0 end) as d_in_Mar_LY
-- from Mar_LY
-- group by ENTITYACCOUNTNUMBER
-- order by ENTITYACCOUNTNUMBER),



FY2018_Donation as (
select distinct ENTITYACCOUNTNUMBER, 
max(case when CampaignYear = 2018 then 1 else 0 end) as d_in_FY2018
from {{ref('fact_time_talent_treasure')}}
group by ENTITYACCOUNTNUMBER
order by ENTITYACCOUNTNUMBER),

FY2019_Donation as (
select distinct ENTITYACCOUNTNUMBER, 
max(case when CampaignYear = 2019 then 1 else 0 end) as d_in_FY2019
from {{ref('fact_time_talent_treasure')}}
group by ENTITYACCOUNTNUMBER
order by ENTITYACCOUNTNUMBER),

FY2020_Donation as (
select distinct ENTITYACCOUNTNUMBER, 
max(case when CampaignYear = 2020 then 1 else 0 end) as d_in_FY2020
from {{ref('fact_time_talent_treasure')}}
group by ENTITYACCOUNTNUMBER
order by ENTITYACCOUNTNUMBER),

FY2021_Donation as (
select distinct ENTITYACCOUNTNUMBER, 
max(case when CampaignYear = 2021 then 1 else 0 end) as d_in_FY2021
from {{ref('fact_time_talent_treasure')}}
group by ENTITYACCOUNTNUMBER
order by ENTITYACCOUNTNUMBER),

all_records as(
    select 
        fy2018.ENTITYACCOUNTNUMBER,
        d_in_FY2018,
        d_in_FY2019,
        d_in_FY2020,
        d_in_FY2021,
        d_in_Apr_TY,
        d_in_May_TY,
        d_in_Jun_TY,
        d_in_Jul_TY,
        d_in_Aug_TY,
        d_in_Sep_TY,
        d_in_Oct_TY,
        d_in_Nov_TY,
        d_in_Dec_TY,
        d_in_Jan_TY,
        d_in_Feb_TY,
        d_in_Mar_TY,
        d_in_Apr_LY,
        d_in_May_LY,
        d_in_Jun_LY,
        d_in_Jul_LY,
        d_in_Aug_LY,
        d_in_Sep_LY,
        d_in_Oct_LY,
        d_in_Nov_LY,
        d_in_Dec_LY,
        d_in_Jan_LY,
        d_in_Feb_LY,
        d_in_Mar_LY
        from FY2018_Donation fy2018 
        full outer join FY2019_Donation fy2019 on fy2018.ENTITYACCOUNTNUMBER=fy2019.ENTITYACCOUNTNUMBER
        full join FY2020_Donation fy2020 on fy2019.ENTITYACCOUNTNUMBER=fy2020.ENTITYACCOUNTNUMBER
        full join FY2021_Donation fy2021 on fy2020.ENTITYACCOUNTNUMBER=fy2021.ENTITYACCOUNTNUMBER
        full join Apr_TY_Donation Apr_ty on fy2021.ENTITYACCOUNTNUMBER=Apr_ty.ENTITYACCOUNTNUMBER
        full join May_TY_Donation May_ty on Apr_ty.ENTITYACCOUNTNUMBER=May_ty.ENTITYACCOUNTNUMBER
        full join Jun_TY_Donation Jun_ty on May_ty.ENTITYACCOUNTNUMBER=Jun_ty.ENTITYACCOUNTNUMBER
        full join Jul_TY_Donation Jul_ty on Jun_ty.ENTITYACCOUNTNUMBER=Jul_ty.ENTITYACCOUNTNUMBER
        full join Aug_TY_Donation Aug_ty on Jul_ty.ENTITYACCOUNTNUMBER=Aug_ty.ENTITYACCOUNTNUMBER
        full join Sep_TY_Donation Sep_ty on Aug_ty.ENTITYACCOUNTNUMBER=Sep_ty.ENTITYACCOUNTNUMBER
        full join Oct_TY_Donation Oct_ty on Sep_ty.ENTITYACCOUNTNUMBER=Oct_ty.ENTITYACCOUNTNUMBER
        full join Nov_TY_Donation Nov_ty on Oct_ty.ENTITYACCOUNTNUMBER=Nov_ty.ENTITYACCOUNTNUMBER
        full join Dec_TY_Donation Dec_ty on Nov_ty.ENTITYACCOUNTNUMBER=Dec_ty.ENTITYACCOUNTNUMBER
        full join Jan_TY_Donation Jan_ty on Dec_ty.ENTITYACCOUNTNUMBER=Jan_ty.ENTITYACCOUNTNUMBER
        full join Feb_TY_Donation Feb_ty on Jan_ty.ENTITYACCOUNTNUMBER=Feb_ty.ENTITYACCOUNTNUMBER
        full join Mar_TY_Donation Mar_ty on Feb_ty.ENTITYACCOUNTNUMBER=Mar_ty.ENTITYACCOUNTNUMBER
        full join Apr_LY_Donation Apr_ly on Mar_ty.ENTITYACCOUNTNUMBER=Apr_ly.ENTITYACCOUNTNUMBER
        full join May_LY_Donation May_ly on Apr_ly.ENTITYACCOUNTNUMBER=May_ly.ENTITYACCOUNTNUMBER
        full join Jun_LY_Donation Jun_ly on May_ly.ENTITYACCOUNTNUMBER=Jun_ly.ENTITYACCOUNTNUMBER
        full join Jul_LY_Donation Jul_ly on Jun_ly.ENTITYACCOUNTNUMBER=Jul_ly.ENTITYACCOUNTNUMBER
        full join Aug_LY_Donation Aug_ly on Jul_ly.ENTITYACCOUNTNUMBER=Aug_ly.ENTITYACCOUNTNUMBER
        full join Sep_LY_Donation Sep_ly on Aug_ly.ENTITYACCOUNTNUMBER=Sep_ly.ENTITYACCOUNTNUMBER
        full join Oct_LY_Donation Oct_ly on Sep_ly.ENTITYACCOUNTNUMBER=Oct_ly.ENTITYACCOUNTNUMBER
        full join Nov_LY_Donation Nov_ly on Oct_ly.ENTITYACCOUNTNUMBER=Nov_ly.ENTITYACCOUNTNUMBER
        full join Dec_LY_Donation Dec_ly on Nov_ly.ENTITYACCOUNTNUMBER=Dec_ly.ENTITYACCOUNTNUMBER
        full join Jan_LY_Donation Jan_ly on Dec_ly.ENTITYACCOUNTNUMBER=Jan_ly.ENTITYACCOUNTNUMBER
        full join Feb_LY_Donation Feb_ly on Jan_ly.ENTITYACCOUNTNUMBER=Feb_ly.ENTITYACCOUNTNUMBER
        full join Mar_LY_Donation Mar_ly on Feb_ly.ENTITYACCOUNTNUMBER=Mar_ly.ENTITYACCOUNTNUMBER
),

all_records_filled_null as (
    select 
        ENTITYACCOUNTNUMBER,
        coalesce(d_in_FY2018,0) as d_in_FY2018,
        coalesce(d_in_FY2019,0) as d_in_FY2019,
        coalesce(d_in_FY2020,0) as d_in_FY2020,
        coalesce(d_in_FY2021,0) as d_in_FY2021,
        coalesce(d_in_Apr_TY,0) as d_in_Apr_TY,
        coalesce(d_in_May_TY,0) as d_in_May_TY,
        coalesce(d_in_Jun_TY,0) as d_in_Jun_TY,
        coalesce(d_in_Jul_TY,0) as d_in_Jul_TY,
        coalesce(d_in_Aug_TY,0) as d_in_Aug_TY,
        coalesce(d_in_Sep_TY,0) as d_in_Sep_TY,
        coalesce(d_in_Oct_TY,0) as d_in_Oct_TY,
        coalesce(d_in_Nov_TY,0) as d_in_Nov_TY,
        coalesce(d_in_Dec_TY,0) as d_in_Dec_TY,
        coalesce(d_in_Jan_TY,0) as d_in_Jan_TY,
        coalesce(d_in_Feb_TY,0) as d_in_Feb_TY,
        coalesce(d_in_Mar_TY,0) as d_in_Mar_TY,
        coalesce(d_in_Apr_LY,0) as d_in_Apr_LY,
        coalesce(d_in_May_LY,0) as d_in_May_LY,
        coalesce(d_in_Jun_LY,0) as d_in_Jun_LY,
        coalesce(d_in_Jul_LY,0) as d_in_Jul_LY,
        coalesce(d_in_Aug_LY,0) as d_in_Aug_LY,
        coalesce(d_in_Sep_LY,0) as d_in_Sep_LY,
        coalesce(d_in_Oct_LY,0) as d_in_Oct_LY,
        coalesce(d_in_Nov_LY,0) as d_in_Nov_LY,
        coalesce(d_in_Dec_LY,0) as d_in_Dec_LY,
        coalesce(d_in_Jan_LY,0) as d_in_Jan_LY,
        coalesce(d_in_Feb_LY,0) as d_in_Feb_LY,
        coalesce(d_in_Mar_LY,0) as d_in_Mar_LY
        from all_records
)

select * from all_records_filled_null 