{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    schema='ebp_sil',
    file_format='delta',
    tblproperties={
      'quality': 'silver',
      'data_domain': 'EBP.OperationalEventsTelemetry',
      'dq_standardized': 'true',
      'deduped': 'true'
    } ) }}

with src as (
  select
    cast(event_id as string)         as event_id,
    cast(event_ts_utc as timestamp)  as event_ts_utc,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc,
    cast(source_system as string)    as source_system,
    cast(engine_id as string)        as engine_id,
    cast(asset_configuration_code as string) as asset_configuration_code,
    cast(site_code as string)        as site_code,
    cast(event_type_code as string)  as event_type_code,
    cast(severity as string)         as severity,
    cast(anomaly_score as double)    as anomaly_score,
    cast(fault_code_count as int)    as fault_code_count,
    cast(threshold_value as double)  as threshold_value,

    to_date(cast(event_ts_utc as timestamp))  as event_date,
    date_format(cast(event_ts_utc as timestamp), 'yyyyMM') as event_yyyymm
  from {{ source('ebp_brz', 'brz_operational_event') }}
  where event_id is not null

  {% if is_incremental() %}
    and cast(ingest_ts_utc as timestamp) >
        (select coalesce(max(ingest_ts_utc), timestamp'1900-01-01') from {{ this }})
  {% endif %}
),

deduped as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (
        partition by event_id
        order by ingest_ts_utc desc, event_ts_utc desc
      ) as rn
    from src
  )
  where rn = 1
)

select * from deduped;
