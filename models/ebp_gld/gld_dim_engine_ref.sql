{{ config(
    materialized = 'incremental',
    schema = 'ebp_gld',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = 'engine_bk_hash',
    tblproperties = { 'quality': 'gold', 'object_type': 'dimension', 'scd_type': '2', 'data_domain': 'EBP.Engine'
    }
) }}

-- Dimension: Engine (SCD Type 2)
-- Business Key: engine_id
-- PK: engine_sk (surrogate)
-- SCD control: current_flag, effective_ts, expiry_ts

with src as (

    -- Source of truth for engine attributes
    select distinct
      engine_id
    from {{ ref('sil_operational_event') }}
    where engine_id is not null
),

prepared as (

    select
      -- Stable business key hash for merge logic
      {{ dbt_utils.generate_surrogate_key(['engine_id']) }} as engine_bk_hash,

      -- Surrogate key (new per SCD version)
      {{ dbt_utils.generate_surrogate_key([
          'engine_id'
      ]) }} as engine_sk,

      engine_id,

      true  as current_flag,
      current_timestamp() as effective_ts,
      cast(null as timestamp) as expiry_ts
    from src
)

select *
from prepared

{% if is_incremental() %}
-- SCD2 behavior:
-- 1) Expire existing current row when attributes change
-- 2) Insert new row with new surrogate key

-- dbt handles this via MERGE using unique_key = engine_bk_hash
{% endif %}
;
