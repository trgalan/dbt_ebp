{{ config(
    materialized = 'streaming_table', file_format  = 'delta', tblproperties = { 'quality': 'bronze', 'data_domain': 'EBP.OperationalEventsTelemetry','pii': 'none',
      'abac_sensitivity': 'Operational','retention': 'raw-immutable','owner_A': 'data-platform','pipelines.autoOptimize.managed': 'true'    }) }}

select
  cast(event_id as string)                    as event_id,
  try_to_timestamp( event_ts_utc,  'yyyy-MM-dd')         as event_ts_utc,
  cast(source_system as string)               as source_system,

  -- Keep the payload column "source_file" as a separate field if it exists in the CSV;
  -- but also capture the true file path from Auto Loader metadata.
  cast(source_file as string)                 as source_file,

  -- Preserve your original "ingest_ts_utc" column if present in CSV, but the authoritative ingest timestamp is from metadata.
  try_to_timestamp( ingest_ts_utc,  'yyyy-MM-dd') AS ingest_ts_utc,

  cast(engine_id as string)                   as engine_id,
  cast(engine_model as string)                as engine_model,
  cast(asset_configuration_code as string)    as asset_configuration_code,
  cast(site_code as string)                   as site_code,

  cast(event_type_code as string)             as event_type_code,
  cast(severity as string)                    as severity,
  cast(anomaly_score as double)               as anomaly_score,
  cast(fault_code_count as int)               as fault_code_count,
  cast(threshold_value as double)             as threshold_value,

  -- (2) ingestion timestamp from file metadata
  _metadata.file_modification_time            as ingest_ts,

  -- (3) file path from file metadata
  _metadata.file_path                         as source_file_path,

  -- (4) rescued data mapping
  _rescued                                    as _rescue

from stream read_files(
  "abfss://root@0815sa.dfs.core.windows.net/p30s_ebp/p30s_ebp_dev/inbound/",
  format => "csv",
  header => "true",
  pathGlobFilter => "oper*.csv",
  rescuedDataColumn => "_rescued"                               
);