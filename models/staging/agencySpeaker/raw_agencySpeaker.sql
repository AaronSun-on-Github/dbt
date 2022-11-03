with raw_agencySpeaker as 
(
    select 
    INDACCOUNTNUMBER,
    INDFIRSTNAME,
    INDMIDDLENAME,
    INDLASTNAME,
    INDACCOUNTSTATUS,
    AFFIL_IND_AGENCYSPKR_ACCOUNTNUMBER,
    AFFIL_IND_AGENCYSPKR_ACCOUNTNAME,
    AFFIL_IND_AGENCYSPKR_EFFECTIVEDATE,
    AFFIL_IND_AGENCYSPKR_EXPIRYDATE
    from {{source('src_ttt','raw_ttt')}}

)

select * from raw_agencySpeaker