DROP TABLE IF EXISTS bronze_bec_ods_stg.RCV_MSH_V;
CREATE TABLE bronze_bec_ods_stg.RCV_MSH_V AS
SELECT
  *
FROM bec_raw_dl_ext.RCV_MSH_V
WHERE
  kca_operation <> 'DELETE';