DROP TABLE IF EXISTS bronze_bec_ods_stg.XXBEC_SRVCREV_CDW_FIXED_STG;
CREATE TABLE bronze_bec_ods_stg.XXBEC_SRVCREV_CDW_FIXED_STG AS
SELECT
  *
FROM bec_raw_dl_ext.XXBEC_SRVCREV_CDW_FIXED_STG
WHERE
  kca_operation <> 'DELETE';