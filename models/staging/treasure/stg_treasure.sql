with stg_treasure_temp as 
(
    select
    ENTITYACCOUNTNUMBER,
    {{get_organization_columns()}},
    Null as INDACCOUNTNUMBER,
    Null as INDFIRSTNAME,
    Null as INDMIDDLENAME,
    Null as INDLASTNAME,
    Null as INDACCOUNTSTATUS,  
    {{get_treasure_columns()}}
    from {{ref('stg_org_treasure')}} 
    union 
    select
    ENTITYACCOUNTNUMBER,
    Null as ORGACCOUNTNUMBER,
    Null as ORGNAME1,
    Null as ORGNAME2,
    {{get_individual_columns()}},
    {{get_treasure_columns()}}
    from {{ref('stg_ind_treasure')}}
),

stg_treasure as
(
    select *, 
    'Treasure' as Level1, 
    TRANS_CAMPAIGNTYPE_18N_LIFE_PROCESSALL_FUNDRAISINGALL as Level2,
    Null as Level3,
    TRANS_TYPE_18N_LIFE_PROCESSALL_FUNDRAISINGALL as Details,
    TRANS_ACCOUNTINGDATE_18N_LIFE_PROCESSALL_FUNDRAISINGALL as Log,
    TRANS_CAMPAIGNYEAR_18N_LIFE_PROCESSALL_FUNDRAISINGALL as CampaignYear
    from stg_treasure_temp 
)

select * from stg_treasure

