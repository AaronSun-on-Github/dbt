with fact_time_talent_treasure as 
(
    select 
    ENTITYACCOUNTNUMBER,
    Log,
    StartDate_to_date,
    EndDate_to_date,
    CampaignYear,
    Level1, 
    Level2, 
    Level3,
    Details,
    Null as TRANS_TOTALPLEDGEAMT_18N_LIFE_PROCESSALL_FUNDRAISINGALL
    from {{ref('stg_time_and_talent_logged')}}
    union
    select
    ENTITYACCOUNTNUMBER,
    Log,
    Null as StartDate_to_date,
    Null as EndDate_to_date,
    CampaignYear,
    Level1, 
    Level2, 
    Null as Level3,
    Details,
    TRANS_TOTALPLEDGEAMT_18N_LIFE_PROCESSALL_FUNDRAISINGALL
    from {{ref('stg_treasure')}}


)

select * from fact_time_talent_treasure