with add_log_stg_time_and_talent_logged as 
(
    select *,
    explode(sequence(StartDate_to_date, EndDate_to_date, interval 1 day)) as Log
    from {{ref('stg_time_and_talent_unlogged')}}
),

stg_time_and_talent_logged as
(
    select* ,
    case
        when log >= '2018-04-01' and log < '2019-04-01' then 2018
        when log >= '2019-04-01' and log < '2020-04-01' then 2019
        when log >= '2020-04-01' and log < '2021-04-01' then 2020
        when log >= '2021-04-01' and log < '2022-04-01' then 2021
        when log >= '2022-04-01' and log < '2023-04-01' then 2022 
    end as CampaignYear
    from add_log_stg_time_and_talent_logged
)

select * from stg_time_and_talent_logged