DROP TABLE IF EXISTS bronze_bec_ods_stg.POBV_BUYERS;
CREATE TABLE bronze_bec_ods_stg.POBV_BUYERS AS
SELECT
  *
FROM bec_raw_dl_ext.POBV_BUYERS
WHERE
  kca_operation <> 'DELETE';