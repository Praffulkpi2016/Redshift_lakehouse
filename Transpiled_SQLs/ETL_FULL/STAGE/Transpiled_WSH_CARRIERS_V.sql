DROP TABLE IF EXISTS bronze_bec_ods_stg.WSH_CARRIERS_V;
CREATE TABLE bronze_bec_ods_stg.WSH_CARRIERS_V AS
SELECT
  *
FROM bec_raw_dl_ext.WSH_CARRIERS_V
WHERE
  kca_operation <> 'DELETE';