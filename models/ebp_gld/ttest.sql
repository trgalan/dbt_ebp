-- Replace with your fully qualified model table name
select *
from system.access.table_lineage
order by event_time desc;

{#
-- models referenced by a model
select
  parent.unique_id     as source_model,
  child.unique_id      as downstream_model
from dbt_artifacts.graph_edges
where child.unique_id = 'models/ebp_brz/brz_operational_event.sql'; 
-----
select
  source_table_full_name,
  target_table_full_name,
  event_time
from system.access.table_lineage
where target_table_full_name = 'models/ebp_brz/brz_operational_event.sql'
order by event_time desc;
#}
