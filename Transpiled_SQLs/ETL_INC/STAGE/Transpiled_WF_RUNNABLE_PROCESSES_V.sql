DROP TABLE IF EXISTS bronze_bec_ods_stg.wf_runnable_processes_v;
CREATE TABLE bronze_bec_ods_stg.wf_runnable_processes_v AS
SELECT
  *
FROM bec_raw_dl_ext.wf_runnable_processes_v
WHERE
  kca_operation <> 'DELETE';