{{ config(
    materialized='table',
    file_format='delta',
    schema='ebp_gld',
    tblproperties={
      'quality': 'gold',
      'object_type': 'dimension',
      'conformed': 'true'
    },
    post_hook=[
      "ALTER TABLE {{ this }} ALTER COLUMN date_sk SET NOT NULL",
      "ALTER TABLE {{ this }} ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_sk)"
    ]
) }}

{# --- best-practice defaults: 10 years past to 10 years future --- #}
{% set years_past   = var('dim_date_years_past', 10) %}
{% set years_future = var('dim_date_years_future', 10) %}

{% set fy_start_month = var('fiscal_year_start_month', 1) %}

with params as (
  select
    add_months(current_date(), -12 * {{ years_past }})   as start_date,
    add_months(current_date(),  12 * {{ years_future }}) as end_date,
    {{ fy_start_month }} as fy_start_month
),
date_spine as (
  select
    explode(sequence(p.start_date, p.end_date, interval 1 day)) as calendar_date,
    p.fy_start_month
  from params p
),
derived as (
  select
    cast(date_format(calendar_date, 'yyyyMMdd') as int) as date_sk,
    cast(calendar_date as date)                        as calendar_date,

    cast(dayofweek(calendar_date) as tinyint)          as day_of_week,
    date_format(calendar_date, 'EEEE')                 as day_name,
    cast(weekofyear(calendar_date) as tinyint)         as week_of_year,

    cast(month(calendar_date) as tinyint)              as month_num,
    date_format(calendar_date, 'MMMM')                 as month_name,

    cast(quarter(calendar_date) as tinyint)            as quarter_num,
    cast(year(calendar_date) as smallint)              as year_num,

    case
      when fy_start_month = 1 then cast(year(calendar_date) as smallint)
      when month(calendar_date) >= fy_start_month then cast(year(calendar_date) + 1 as smallint)
      else cast(year(calendar_date) as smallint)
    end as fiscal_year,

    cast(
      case
        when fy_start_month = 1 then month(calendar_date)
        when month(calendar_date) >= fy_start_month then month(calendar_date) - fy_start_month + 1
        else month(calendar_date) + (12 - fy_start_month + 1)
      end as tinyint
    ) as fiscal_month_in_year
  from date_spine
)

select
  date_sk,
  calendar_date,
  day_of_week,
  day_name,
  week_of_year,
  month_num,
  month_name,
  quarter_num,
  year_num,
  concat('FY', cast(fiscal_year as string), '-P', lpad(cast(fiscal_month_in_year as string), 2, '0')) as fiscal_period,
  (dayofweek(calendar_date) between 2 and 6) as is_workday,
  false as is_holiday
from derived
where date_sk is not null
;
