{{ config(
  materialized='streaming_table',
  schema='dev_bronze',
  alias='brz_engines',
  tblproperties={
    'quality': 'bronze',
    'pipelines.autoOptimize.managed': 'true'
  }
) }}

with src as (
  select
    cast(engine_id as string)            as engine_id,
    cast(engine_model_id as string)      as engine_model_id,
    cast(serial_number as string)        as serial_number,
    cast(thrust_class as string)         as thrust_class,
    cast(operator_customer as string)    as operator_customer,
    to_date(in_service_date, 'M/d/yyyy') as in_service_date,
    cast(configuration_status as string) as configuration_status,
    current_timestamp()                  as ingest_ts,
    'dbt_ebp.raw_data.engine_01'         as source_table
  from STREAM({{ source('raw_data', 'engine_01') }})
)
select *
from src
where engine_id is not null;
