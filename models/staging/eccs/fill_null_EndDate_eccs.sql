with fill_null_EndDate_eccs as

(
    select 
    INDACCOUNTNUMBER,
    INDFIRSTNAME,
    INDMIDDLENAME,
    INDLASTNAME,
    INDACCOUNTSTATUS,
    CNT_BOTH_ECC_CONTACTTYPE,
    CNT_BOTH_ECC_ACCOUNTNUMBER,
    CNT_BOTH_ECC_SORTFIELD,
    CNT_BOTH_ECC_LASTNAME,
    CNT_BOTH_ECC_FIRSTNAME,
    CNT_BOTH_ECC_EFFECTIVEDATE,
    CNT_BOTH_ECC_EXPIRYDATE,
    StartDate,
    coalesce(EndDate, StartDate) as EndDate,
    StartDateLen,
    StartDate_to_date

    from {{ref('add_column_StartDate_to_date_eccs')}}
)

select * from fill_null_EndDate_eccs

