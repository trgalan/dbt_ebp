{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='engine_id',
    file_format='delta',

    tblproperties={
      'quality': 'silver',
      'pipelines.autoOptimize.managed': 'true',
      'delta.liquidClustering.enabled': 'true'
    },

    post_hook=[
      "ALTER TABLE {{ this }} CLUSTER BY (engine_id)"
    ]
) }}

WITH src AS (
  SELECT
    CAST(engine_id            AS STRING)    AS engine_id,
    CAST(engine_model_id      AS STRING)    AS engine_model_id,
    CAST(serial_number        AS STRING)    AS serial_number,
    CAST(thrust_class         AS STRING)    AS thrust_class,
    CAST(operator_customer    AS STRING)    AS operator_customer,
    CAST(in_service_date      AS DATE)      AS in_service_date,
    CAST(configuration_status AS STRING)    AS configuration_status,
    CAST(ingest_ts            AS TIMESTAMP) AS ingest_ts,
    CAST(source_file          AS STRING)    AS source_file,
    _rescue
  FROM {{ source('brz', 'brz_engines_auto_dlt') }}   --FROM {{ ref('brz_engines') }}
),

incoming AS (
  -- Dedup within the incoming set so merge work is minimal and deterministic
  SELECT *
  FROM src
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY engine_id
    ORDER BY ingest_ts DESC, source_file DESC
  ) = 1
)

SELECT * FROM incoming
