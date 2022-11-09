with filter_org_records_ind_treasure as 
(
    select * from {{ref('raw_ind_treasure')}} where INDACCOUNTNUMBER != '0'
),

drop_accounting_date_is_null_ind_treasure as
(
    select * from filter_org_records_ind_treasure 
    where TRANS_ACCOUNTINGDATE_18N_LIFE_PROCESSALL_FUNDRAISINGALL is not null
),

drop_duplicate_ind_treasure as 
(
    select distinct * from drop_accounting_date_is_null_ind_treasure
),

stg_ind_treasure as 
(
    select *,
    INDACCOUNTNUMBER as ENTITYACCOUNTNUMBER
    from drop_duplicate_ind_treasure
)

select * from stg_ind_treasure
