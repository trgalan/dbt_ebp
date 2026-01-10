-- models/ebp_gld/dim_time.sql
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
      "ALTER TABLE {{ this }} ALTER COLUMN time_sk SET NOT NULL",
      "ALTER TABLE {{ this }} ALTER COLUMN clock_time SET NOT NULL",
      "ALTER TABLE {{ this }} ADD CONSTRAINT pk_dim_time PRIMARY KEY (time_sk)"
    ]
) }}

with time_spine as (
  select explode(sequence(0, 86399, 1)) as seconds_from_midnight
),
derived as (
  select
    -- PK: HHMMSS (INT)
    cast(
      lpad(cast(floor(seconds_from_midnight / 3600) as string), 2, '0') ||
      lpad(cast(floor((seconds_from_midnight % 3600) / 60) as string), 2, '0') ||
      lpad(cast((seconds_from_midnight % 60) as string), 2, '0')
    as int) as time_sk,

    -- "HH:mm:ss"
    concat(
      lpad(cast(floor(seconds_from_midnight / 3600) as string), 2, '0'), ':',
      lpad(cast(floor((seconds_from_midnight % 3600) / 60) as string), 2, '0'), ':',
      lpad(cast((seconds_from_midnight % 60) as string), 2, '0')
    ) as clock_time,

    cast(floor(seconds_from_midnight / 3600) as tinyint)        as hour_24,
    cast(floor((seconds_from_midnight % 3600) / 60) as tinyint) as minute_num,
    cast((seconds_from_midnight % 60) as tinyint)               as second_num
  from time_spine
)

select
  time_sk,
  clock_time,
  hour_24,
  minute_num,
  second_num,
  case
    when hour_24 between 6 and 13  then 'D1'
    when hour_24 between 14 and 21 then 'D2'
    else 'N'
  end as shift_code
from derived
;
