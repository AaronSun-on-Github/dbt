with filter_ind_records_org_treasure as 
(
    select * from {{ref('raw_org_treasure')}} where ORGACCOUNTNUMBER != '0'
),

drop_accounting_date_is_null_org_treasure as 
(
    select * 
    from filter_ind_records_org_treasure
    where TRANS_ACCOUNTINGDATE_18N_LIFE_PROCESSALL_FUNDRAISINGALL is not null
),

drop_duplicate_org_treasure as 
(
    select distinct * 
    from drop_accounting_date_is_null_org_treasure
),

stg_org_treasure as 
(
    select *,
    ORGACCOUNTNUMBER as ENTITYACCOUNTNUMBER
    from drop_duplicate_org_treasure
)

select * from stg_org_treasure