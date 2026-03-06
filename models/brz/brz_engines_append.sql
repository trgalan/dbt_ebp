{{ config(
    materialized='incremental',
    file_format='delta',
    incremental_strategy='append'
) }}

with src as (
  select *
  from {{ source('raw_data', 'engine_01') }}
),
staged as (
  select
    cast(engine_id as string)            as engine_id,
    cast(engine_model_id as string)      as engine_model_id,
    cast(serial_number as string)        as serial_number,
    cast(thrust_class as string)         as thrust_class,
    cast(operator_customer as string)    as operator_customer,
    to_date(in_service_date, 'M/d/yyyy') as in_service_date,
    cast(configuration_status as string) as configuration_status,

    cast(current_timestamp()  as timestamp)        as arrival_ts,
    current_timestamp()                  as ingest_ts,
    'raw_data.engine_01'                 as source_relation
  from src
)

select *
from staged

{% if is_incremental() %}
where staged.arrival_ts >
       (select max(arrival_ts) from {{ this }})
{% endif %}
;
