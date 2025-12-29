{{ config(
    materialized='incremental',
    file_format='delta',
    incremental_strategy='append'
) }}

-- Append-only raw landing:
-- No is_incremental() filter because there is no reliable arrival watermark.
-- Late arrivals are simply appended whenever they appear upstream.

with src as (
  select *
  from {{ source('raw_data', 'engine_01') }}
)

select
  cast(engine_id as string)            as engine_id,
  cast(engine_model_id as string)      as engine_model_id,
  cast(serial_number as string)        as serial_number,
  cast(thrust_class as string)         as thrust_class,
  cast(operator_customer as string)    as operator_customer,
  to_date(in_service_date, 'M/d/yyyy') as in_service_date,
  cast(configuration_status as string) as configuration_status,

  -- Keep an ingestion timestamp for observability only (NOT a watermark)
  current_timestamp()                  as ingest_ts,

  -- Optional row provenance
  'raw_data.engine_01'                 as source_relation
from src
;
