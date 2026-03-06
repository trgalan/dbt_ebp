{% snapshot sil_engine_ref__snap %}

{{ 
  config(
    target_schema = 'snapshots',
    unique_key    = 'engine_id',
    strategy      = 'timestamp',
    updated_at    = 'ingest_ts'
  ) 
}}

select *
from {{ ref('sil_engine_ref') }}

{% endsnapshot %}
