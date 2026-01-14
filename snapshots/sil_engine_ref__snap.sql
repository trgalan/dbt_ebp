{% snapshot sil_engine_ref_snap %} 

{{

    config {
        target_schema = 'snapshots',
        unique_key = 'engine_id',
        strategy = 'timestamp',
        updated_at = 'ingest_ts'

    }

}}

SELECT * FROM  {{ {{ ref('sil_engine_ref') }}   }}

{%  endsnapshot %}

