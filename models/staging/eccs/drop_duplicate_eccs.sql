with drop_duplicate_eccs as (
       

        {{dbt_utils.deduplicate(
          relation=ref('raw_eccs'),
          partition_by='INDACCOUNTNUMBER,
          CNT_BOTH_ECC_CONTACTTYPE,
          CNT_BOTH_ECC_ACCOUNTNUMBER,
          CNT_BOTH_ECC_SORTFIELD,
          CNT_BOTH_ECC_LASTNAME,
          CNT_BOTH_ECC_FIRSTNAME,
          CNT_BOTH_ECC_EFFECTIVEDATE,
          CNT_BOTH_ECC_EXPIRYDATE',
          order_by= 'INDACCOUNTNUMBER'
          )
        }}
        
)

select * from drop_duplicate_eccs