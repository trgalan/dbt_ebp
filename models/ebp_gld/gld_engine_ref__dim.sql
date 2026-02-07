-- Model: gld_engine_ref__dim (NON-SCD)
-- Business Key: engine_id
-- Grain: 1 row per (engine_id, engine_model)
-- PK: engine_sk (surrogate, deterministic)
-- Desc:
{{ config(
    materialized         = 'incremental',
    schema               = 'ebp_gld',
    file_format          = 'delta',
    incremental_strategy = 'merge',
    unique_key           = ['engine_id', 'engine_model'],
    on_schema_change     = 'sync_all_columns',
    tblproperties        = {
      'quality': 'gold',
      'object_type': 'dimension',
      'data_domain': 'EBP.Engine'
    },

    post_hook = [
      "ALTER TABLE {{ this }} ALTER COLUMN engine_sk SET NOT NULL"
      
    ]
) }}

with src as (
  select
    engine_id,
    engine_model,
    thrust_class
  from {{ ref('sil_engine_ref') }}
  where engine_id is not null
    and engine_model is not null
),

dedup as (
  select
    engine_id,
    engine_model,
    thrust_class,
    row_number() over (
      partition by engine_id, engine_model
      order by engine_id, engine_model
    ) as rn
  from src
)

select
  -- ensure dbt produces a non-null value in all cases
  {{ dbt_utils.generate_surrogate_key(['engine_id','engine_model']) }} as engine_sk,
  engine_id,
  engine_model,
  thrust_class,
  true                    as current_flag,
  current_timestamp()     as effective_ts,
  cast(null as timestamp) as expiry_ts
from dedup
where rn = 1
;
