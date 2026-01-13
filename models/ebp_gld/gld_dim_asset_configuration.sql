-- models/gld/gld_dim_asset_configuration.sql

{{ config(
    materialized = 'table',
    schema = 'ebp_gld',
    file_format = 'delta',
    tblproperties = {
      'quality': 'gold',
      'object_type': 'dimension',
      'data_domain': 'EBP.AssetConfiguration'
    }
) }}

-- Dimension: Asset Configuration (Type 1)
-- PK: asset_config_sk (surrogate)
-- BK: asset_configuration_code (NOT NULL)

with src as (

    select distinct
      cast(asset_configuration_code as string) as asset_configuration_code,
      cast(mod_state as string)                as mod_state,
      cast(build_standard as string)           as build_standard,
      cast(compliance_state as string)         as compliance_state
    from {{ ref('sil_operational_event') }}
    where asset_configuration_code is not null

),

deduped as (

    -- If mod_state/build_standard/compliance_state aren't in the silver source yet,
    -- they'll resolve to NULL and can be enriched later from master data.
    select
      asset_configuration_code,
      max(mod_state)        as mod_state,
      max(build_standard)   as build_standard,
      max(compliance_state) as compliance_state
    from src
    group by asset_configuration_code
)

select
  -- Surrogate key (deterministic, dbt-managed)
  {{ dbt_utils.generate_surrogate_key(['asset_configuration_code']) }} as asset_config_sk,

  asset_configuration_code,
  mod_state,
  build_standard,
  compliance_state
from deduped;
