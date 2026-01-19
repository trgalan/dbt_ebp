
{{ config(
    materialized = 'incremental',
    schema = 'ebp_gld',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id','ingest_ts_utc'],
    tblproperties = {
      'quality': 'gold', 'object_type': 'fact', 'grain': '1 row per operational event_id', 'data_domain': 'EBP.OperationalEventsTelemetry'
    }
) }}

-- Grain: 1 row per operational event_id  -- PK (logical): event_id  -- FKs: date_sk, time_sk, engine_sk, asset_config_sk, site_sk, event_type_sk

with src as (

    select  event_id, event_ts_utc,  ingest_ts_utc, source_system, engine_id, asset_configuration_code, site_code, event_type_code,
      anomaly_score, fault_code_count, threshold_value
    from {{ ref('sil_operational_event') }}
    where event_id is not null

),
resolved as (
    select
      s.event_id,
      -- Conformed date / time keys
      cast(date_format(to_date(s.event_ts_utc), 'yyyyMMdd') as int)       as date_sk,
      cast(replace(date_format(s.event_ts_utc, 'HH:mm:ss'), ':', '') as int) as time_sk,
      -- Dimension surrogate keys (resolved at load time)
      e.engine_sk,
      ac.asset_config_sk,
      si.site_sk,
      et.event_type_sk,
      -- Degenerate / descriptive fields
      s.source_system,
      -- True timestamps
      s.event_ts_utc,
      s.ingest_ts_utc,

      -- Measures
      1                      as event_count,
      s.anomaly_score,
      s.fault_code_count,
      s.threshold_value

    from src s
    left join {{ ref('gld_engine_ref__dim') }} e
      on s.engine_id = e.engine_id
    
    left join {{ ref('gld_dim_asset_configuration') }} ac
      on s.asset_configuration_code = ac.asset_configuration_code

    left join {{ ref('gld_dim_site') }} si
      on s.site_code = si.site_code

    left join {{ ref('gld_dim_event_type') }} et
      on s.event_type_code = et.event_type_code
    
)

select
  -- NOTE:
  -- operational_event_sk is intentionally NOT generated in dbt SQL.
  -- Databricks Delta will assign it automatically if the table is created
  -- with an IDENTITY column, or it can be omitted entirely in dbt-managed tables.

  event_id,  date_sk,  time_sk,  engine_sk,  asset_config_sk,  site_sk,  event_type_sk,  source_system, event_ts_utc,
  ingest_ts_utc, event_count, anomaly_score, fault_code_count, threshold_value

from resolved
{#
{% if is_incremental() %}
where ingest_ts_utc >
      (select coalesce(max(ingest_ts_utc), timestamp('1900-01-01')) from {{ this }})
{% endif %}
#}
;
