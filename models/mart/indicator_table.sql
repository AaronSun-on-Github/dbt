{% set d_in_TY_columns = get_d_in_TY_columns() %}
{% set d_in_LY_columns = get_d_in_LY_columns() %}


with indicator_table_with_segmentation as (
    select *,
    {% for d_in_TY_column in d_in_TY_columns %}
    case
    when (
      d_in_FY2018 = 0
      and d_in_FY2019 = 0
      and d_in_FY2020 = 0
      and d_in_FY2021 = 0
      and {{d_in_TY_column}} = 1 --change
    ) then 'New_Customer'
    when (
      d_in_FY2021 = 1
      and {{d_in_TY_column}} = 1 --change
    ) then 'Retained_Customer'
    when (
      (
        d_in_FY2018 = 1
        or d_in_FY2019 = 1
        or d_in_FY2020 = 1
      )
      and d_in_FY2021 = 0
      and {{d_in_TY_column}} = 1 --change
    ) then 'Recovered_Customer'
    when (
      d_in_FY2021 = 1
      and {{d_in_TY_column}} = 0 --change
    ) then 'Outstanding_Customer'
    when (
      (
        d_in_FY2018 = 1
        or d_in_FY2019 = 1
        or d_in_FY2020 = 1
      )
      and d_in_FY2021 = 0
      and {{d_in_TY_column}} = 0 --change
    ) then 'Lapsed_Customer'
    else 'Lost_Customer'
    end as Segmentation_upon_{{d_in_TY_column}}, --change
    {%endfor%}

    {%for d_in_LY_column in d_in_LY_columns%}
      case
    when (
      d_in_FY2020 = 1
      and {{d_in_LY_column}} = 0 --change
    ) then 'Outstanding_LY'
    when ({{d_in_LY_column}} = 1) --change
    then 'Donated_Customer_LY'
    else 'other_LY'
    end as Segmentation_upon_{{d_in_LY_column}}
    {%- if not loop.last %},{% endif -%}
    {%endfor%}

    from {{ref('indicator_table')}}
)

select * from indicator_table_with_segmentation