-- models/gld/gld_dim_event_type.sql

{{ config(
    materialized = 'table',
    schema = 'ebp_gld',
    file_format = 'delta',
    tblproperties = {
      'quality': 'gold',
      'object_type': 'dimension',
      'conformed': 'true',
      'data_domain': 'EBP.Event'
    }
) }}

-- Dimension: Event Type (Type 1, Conformed)
-- PK: event_type_sk (surrogate)
-- BK: event_type_code (NOT NULL)

with src as (

    -- Conformed capture of all event types observed in operational telemetry
    select distinct
      cast(event_type_code as string) as event_type_code
    from {{ ref('sil_operational_event') }}
    where event_type_code is not null

    {# Optional: if you also want decisioning event types conformed, uncomment
    union
    select distinct cast(event_type_code as string) as event_type_code
    from {{ ref('sil_ebp_decision') }}
    where event_type_code is not null
    #}
),

enriched as (

    -- Minimal deterministic enrichment using code-based bucketing.
    -- Replace with joins to a true reference/master mapping table when available.
    select
      event_type_code,

      case
        when upper(event_type_code) in ('FAIL_SIG','FAILURE','FAILURE_SIGNAL') then 'telemetry'
        when upper(event_type_code) in ('STATUS_CHG','STATUS_CHANGE') then 'production'
        when upper(event_type_code) in ('DEMAND_SPIKE','DEMAND') then 'planning'
        when upper(event_type_code) in ('MAINT','MAINTENANCE') then 'maintenance'
        else null
      end as event_domain,

      case
        when upper(event_type_code) in ('DEMAND_SPIKE','DEMAND') then 'demand_spike'
        when upper(event_type_code) in ('FAIL_SIG','FAILURE','FAILURE_SIGNAL','ANOMALY') then 'failure_signal'
        when upper(event_type_code) in ('STATUS_CHG','STATUS_CHANGE') then 'status_change'
        else null
      end as trigger_class,

      case
        when upper(event_type_code) in ('FAIL_SIG','FAILURE','FAILURE_SIGNAL') then 'high'
        when upper(event_type_code) in ('ANOMALY') then 'medium'
        else null
      end as severity_band

    from src
)

select
  -- Surrogate key (deterministic, dbt-managed)
  {{ dbt_utils.generate_surrogate_key(['event_type_code']) }} as event_type_sk,

  event_type_code,
  event_domain,
  trigger_class,
  severity_band
from enriched;
