DROP TABLE IF EXISTS bronze_bec_ods_stg.CST_COST_HISTORY_V;
CREATE TABLE bronze_bec_ods_stg.CST_COST_HISTORY_V AS
SELECT
  *
FROM bec_raw_dl_ext.CST_COST_HISTORY_V
WHERE
  kca_operation <> 'DELETE';