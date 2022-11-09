with stg_time_and_talent_unlogged_temp as 
(
    select {{get_common_columns()}}, Level3 from {{ref('stg_affinityGroups')}}
    union
    select {{get_common_columns()}}, Null as Level3 from {{ref('stg_agencySpeaker')}}
    union
    select {{get_common_columns()}}, Level3 from {{ref('stg_attendees')}}
    union
    select {{get_common_columns()}}, Level3 from {{ref('stg_eccms')}}
    union
    select {{get_common_columns()}}, Level3 from {{ref('stg_eccs')}}
    union
    select {{get_common_columns()}}, Null as Level3 from {{ref('stg_opportunities')}}
    
),

stg_time_and_talent_unlogged as 
(
    select
    INDACCOUNTNUMBER as ENTITYACCOUNTNUMBER,
    Level1, 
    Level2, 
    Level3,
    StartDate_to_date,
    EndDate_to_date,
    Details
    from stg_time_and_talent_unlogged_temp
)

select * from stg_time_and_talent_unlogged