{{ config(
    materialized='incremental',
    file_format='delta',
    incremental_strategy='append'
) }}

-- Append-only raw landing using an arrival watermark.
-- arrival_ts is a DATE-only watermark normalized to midnight (00:00:00).

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

  -- DATE-only arrival watermark persisted as TIMESTAMP at 00:00:00
  to_timestamp(current_date())         as arrival_ts,

  -- Optional operational observability (not used for watermarking)
  current_timestamp()                  as ingest_ts,

  -- Optional row provenance
  'raw_data.engine_01'                 as source_relation

from src

{% if is_incremental() %}
where to_timestamp(current_date()) >
      (select max(arrival_ts) from {{ this }})
{% endif %}
;
