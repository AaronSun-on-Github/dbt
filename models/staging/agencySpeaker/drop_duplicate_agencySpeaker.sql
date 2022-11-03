with drop_duplicate_agencySpeaker as 
(
        {{dbt_utils.deduplicate(
          relation=ref('raw_agencySpeaker'),
          partition_by='INDACCOUNTNUMBER,
            INDFIRSTNAME,
            INDMIDDLENAME,
            INDLASTNAME,
            INDACCOUNTSTATUS,
            AFFIL_IND_AGENCYSPKR_ACCOUNTNUMBER,
            AFFIL_IND_AGENCYSPKR_ACCOUNTNAME,
            AFFIL_IND_AGENCYSPKR_EFFECTIVEDATE,
            AFFIL_IND_AGENCYSPKR_EXPIRYDATE',
          order_by= 'INDACCOUNTNUMBER,
            INDFIRSTNAME,
            INDMIDDLENAME,
            INDLASTNAME,
            INDACCOUNTSTATUS,
            AFFIL_IND_AGENCYSPKR_ACCOUNTNUMBER,
            AFFIL_IND_AGENCYSPKR_ACCOUNTNAME,
            AFFIL_IND_AGENCYSPKR_EFFECTIVEDATE,
            AFFIL_IND_AGENCYSPKR_EXPIRYDATE'
          )
        }}    
)

select * from drop_duplicate_agencySpeaker