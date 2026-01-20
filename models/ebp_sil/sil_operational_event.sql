{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = ['event_id','event_ts_utc'],
    schema               = 'ebp_sil',
    file_format          = 'delta',
    tblproperties        = {
      'quality': 'silver','data_domain': 'EBP.OperationalEventsTelemetry','dq_standardized': 'true', 'deduped': 'true'
    }
) }}

with src_raw as (
  select
    cast(event_id as string)         as event_id,
    cast(event_ts_utc as timestamp)  as event_ts_utc,
    ingest_ts                        as ingest_ts_utc, -- _metadata.file_modification_time from brz
    cast(source_system as string)    as source_system,
    cast(engine_id as string)        as engine_id,
    cast(asset_configuration_code as string) as asset_configuration_code,
    cast(site_code as string)        as site_code,
    cast(event_type_code as string)  as event_type_code,
    cast(severity as string)         as severity,
    cast(anomaly_score as double)    as anomaly_score,
    cast(fault_code_count as int)    as fault_code_count,
    cast(threshold_value as double)  as threshold_value        

  from {{ ref('brz_operational_event') }}
  where event_id is not null
    and event_ts_utc is not null
    and ingest_ts is not null

    {% if is_incremental() %}
      and ingest_ts >
          ( select max(ingest_ts_utc) from {{ this }} )
    {% endif %}

),
src as (
  -- ensure ONE row per merge key per run to avoid DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
  select *
  from src_raw
  qualify row_number() over (
    partition by event_id, event_ts_utc
    order by ingest_ts_utc desc
  ) = 1
)
select
  event_id, event_ts_utc, ingest_ts_utc,   -- SIL load time (current run)
  source_system, engine_id, asset_configuration_code, site_code, event_type_code, severity, anomaly_score, fault_code_count,  threshold_value,
  to_date(ingest_ts_utc)               as event_date,
  date_format(ingest_ts_utc, 'yyyyMM') as event_yyyymm

from src
