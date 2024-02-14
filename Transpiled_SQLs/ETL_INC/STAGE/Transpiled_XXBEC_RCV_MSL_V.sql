DROP TABLE IF EXISTS bronze_bec_ods_stg.XXBEC_RCV_MSL_V;
CREATE TABLE bronze_bec_ods_stg.XXBEC_RCV_MSL_V AS
SELECT
  *
FROM bec_raw_dl_ext.XXBEC_RCV_MSL_V
WHERE
  kca_operation <> 'DELETE';