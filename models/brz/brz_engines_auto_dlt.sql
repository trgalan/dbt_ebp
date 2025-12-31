{{ config(
    materialized = "streaming_table",
    schema = "dbt_ebp",
    alias  = "brz_engines_auto_dlt",
    tblproperties = {
      "quality": "bronze",
      "pipelines.autoOptimize.managed": "true"
    }
) }}

SELECT
  CAST(engine_id            AS STRING)            AS engine_id,
  CAST(engine_model_id      AS STRING)            AS engine_model_id,
  CAST(serial_number        AS STRING)            AS serial_number,
  CAST(thrust_class         AS STRING)            AS thrust_class,
  CAST(operator_customer    AS STRING)            AS operator_customer,
  TO_DATE(in_service_date, 'M/d/yyyy')            AS in_service_date,
  CAST(configuration_status AS STRING)            AS configuration_status,

  _metadata.file_modification_time                AS ingest_ts,
  _metadata.file_path                             AS source_file,
  _rescued                                        AS _rescue

FROM STREAM read_files(
  -- Prefer the *directory* for streaming ingestion (more reliable than a single file path)
  "abfss://root@0815sa.dfs.core.windows.net/p30s_ebp/p30s_ebp_dev/inbound/",
  format => "csv",
  header => "true",

  -- Explicit schema => no inference => no CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE
  schema => "engine_id STRING,
             engine_model_id STRING,
             serial_number STRING,
             thrust_class STRING,
             operator_customer STRING,
             in_service_date STRING,
             configuration_status STRING",

  schemaEvolutionMode => "addNewColumns",
  rescuedDataColumn   => "_rescued",
  mode                => "PERMISSIVE"
);
