{{ config(
    materialized='streaming_table',
    schema='dbt_brz',
    alias='brz_engines_auto_dlt',
    tblproperties={
      'quality': 'bronze',
      'pipelines.autoOptimize.managed': 'true'
    }
) }}

SELECT
  CAST(engine_id            AS STRING) AS engine_id,
  CAST(engine_model_id      AS STRING) AS engine_model_id,
  CAST(serial_number        AS STRING) AS serial_number,
  CAST(thrust_class         AS STRING) AS thrust_class,
  CAST(operator_customer    AS STRING) AS operator_customer,

  try_cast(
    CASE
      WHEN in_service_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN in_service_date
      WHEN in_service_date RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN
        concat(
          regexp_extract(in_service_date, '([0-9]{4})$', 1), '-',
          lpad(regexp_extract(in_service_date, '^([0-9]{1,2})/', 1), 2, '0'), '-',
          lpad(regexp_extract(in_service_date, '/([0-9]{1,2})/', 1), 2, '0')
        )
      ELSE NULL
    END
    AS DATE
  ) AS in_service_date,

  CAST(configuration_status AS STRING) AS configuration_status,
  _metadata.file_modification_time AS ingest_ts,
  _metadata.file_path              AS source_file,
  _rescued                         AS _rescue

FROM STREAM read_files(
  "abfss://root@0815sa.dfs.core.windows.net/p30s_ebp/p30s_ebp_dev/inbound/",
  format => "csv",
  header => "true",
  schema => "engine_id STRING,
             engine_model_id STRING,
             serial_number STRING,
             thrust_class STRING,
             operator_customer STRING,
             in_service_date STRING,
             configuration_status STRING",
  rescuedDataColumn => "_rescued",
  mode => "PERMISSIVE"
);
