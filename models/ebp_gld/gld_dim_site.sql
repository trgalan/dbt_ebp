{{ config(
    materialized = 'table',
    schema = 'ebp_gld',
    file_format = 'delta',
    tblproperties = {
      'quality': 'gold',
      'object_type': 'dimension',
      'conformed': 'true',
      'data_domain': 'EBP.Site'
    }
) }}

-- Dimension: Site (Type 1, Conformed)
-- PK: site_sk (surrogate)
-- BK: site_code (NOT NULL)

with src as (
    -- Collect site codes from all silver fact sources
    select distinct
      cast(site_code as string) as site_code
    from {{ ref('sil_operational_event') }}
    where site_code is not null
    -- union
),

deduped as (

    -- Type-1 behavior: one row per site_code
    select
      site_code,
      max(site_name) as site_name,
      max(plant)     as plant,
      max(region)    as region
    from (
        select
          site_code,
          null as site_name,
          null as plant,
          null as region
        from src
    )
    group by site_code
)

select
  -- Surrogate key (deterministic, dbt-managed)
  {{ dbt_utils.generate_surrogate_key(['site_code']) }} as site_sk,

  site_code,
  site_name,
  plant,
  region
from deduped;
