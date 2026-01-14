{{ config(
    materialized         = 'incremental',
    schema               = 'ebp_sil',
    file_format          = 'delta',
    incremental_strategy = 'merge',
    unique_key           = ['engine_id','effective_ts'],
    on_schema_change     = 'sync_all_columns',
    tblproperties = {
      'quality' : 'silver',
      'data_domain' : 'EBP.EngineMaster',
      'pipelines.autoOptimize.managed' : 'true'
    }
) }}

with src as (
  select
    cast(engine_id    as string)    as engine_id,
    cast(engine_model as string)    as engine_model,
    cast(thrust_class as string)    as thrust_class,
    cast(current_flag as boolean)   as current_flag,

    cast(effective_ts as timestamp) as effective_ts,
    cast(expiry_ts    as timestamp) as expiry_ts,

    cast(ingest_ts as timestamp)     as ingest_ts,
    cast(source_file_path as string) as source_file_path,
    _rescue
  from {{ ref('brz_engine_ref') }}
  where engine_id is not null
    and effective_ts is not null
),

-- keep latest per (engine_id, effective_ts)
dedup as (
  select *
  from (
    select
      s.*,
      row_number() over (
        partition by s.engine_id, s.effective_ts
        order by s.ingest_ts desc, s.source_file_path desc
      ) as rn
    from src s
  )
  where rn = 1
)

select
  engine_id,
  engine_model,
  thrust_class,
  current_flag,
  effective_ts,
  expiry_ts,
  ingest_ts,
  source_file_path,
  _rescue
from dedup;
