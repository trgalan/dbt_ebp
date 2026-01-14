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
      cast(asset_configuration_code as string) as asset_configuration_code
    from {{ ref('sil_operational_event') }}
    where asset_configuration_code is not null

),

deduped as (

    select
      asset_configuration_code
    from src
    group by asset_configuration_code
)

select
  -- Surrogate key (deterministic, dbt-managed)
  {{ dbt_utils.generate_surrogate_key(['asset_configuration_code']) }} as asset_config_sk,
  asset_configuration_code
from deduped;
