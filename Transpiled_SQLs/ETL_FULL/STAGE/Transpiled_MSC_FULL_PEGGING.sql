DROP TABLE IF EXISTS bronze_bec_ods_stg.MSC_FULL_PEGGING;
CREATE TABLE bronze_bec_ods_stg.MSC_FULL_PEGGING AS
SELECT
  *
FROM bec_raw_dl_ext.MSC_FULL_PEGGING
WHERE
  kca_operation <> 'DELETE';