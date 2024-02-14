DROP TABLE IF EXISTS bronze_bec_ods_stg.wsh_deliverables_v;
CREATE TABLE bronze_bec_ods_stg.wsh_deliverables_v AS
SELECT
  *
FROM bec_raw_dl_ext.wsh_deliverables_v
WHERE
  kca_operation <> 'DELETE';