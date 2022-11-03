with add_StartDate_EndDate_column_agencySpeaker as 

(
    select *, 
    AFFIL_IND_AGENCYSPKR_EFFECTIVEDATE as StartDate,
    AFFIL_IND_AGENCYSPKR_EXPIRYDATE as EndDate

    from {{ref('drop_duplicate_agencySpeaker')}}
)

select * from add_StartDate_EndDate_column_agencySpeaker