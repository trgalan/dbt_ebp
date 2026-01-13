{{ config(
    materialized = 'streaming_table',
    schema = 'ebp_brz',
    file_format = 'delta',
    tblproperties = {
      'quality': 'bronze', 'data_domain': 'EBP.EngineMaster', 'retention': 'raw-immutable', 'pipelines.autoOptimize.managed': 'true'
    }
) }}

select
  -- Raw engine master attributes (cast only)
  cast(engine_id    as string)  as engine_id,
  cast(engine_model as string)  as engine_model,
  cast(thrust_class as string)  as thrust_class,

  cast(current_flag as boolean) as current_flag,

  -- ISO-8601 UTC timestamps with Z suffix
  try_to_timestamp(effective_ts) as effective_ts,
  try_to_timestamp(expiry_ts)    as expiry_ts,

  -- ingestion timestamp from file metadata
  _metadata.file_modification_time as ingest_ts,

  -- file path from file metadata
  _metadata.file_path              as source_file_path,

  -- rescued data mapping
  _rescued                          as _rescue

from stream read_files(
  "abfss://root@0815sa.dfs.core.windows.net/p30s_ebp/p30s_ebp_dev/inbound/engine_ref",
  format => "csv",
  header => "true",
  pathGlobFilter => "engine_ref_*.csv",
  rescuedDataColumn => "_rescued"
);
