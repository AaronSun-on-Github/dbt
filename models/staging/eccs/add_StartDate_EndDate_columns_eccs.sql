with add_StartDate_EndDate_columns_eccs as 

(
    select *, 
    CNT_BOTH_ECC_EFFECTIVEDATE as StartDate,
    CNT_BOTH_ECC_EXPIRYDATE as EndDate

    from {{ref('drop_duplicate_eccs')}}
)

select * from add_StartDate_EndDate_columns_eccs
-- select count (*) from add_StartDate_EndDate_columns_eccs