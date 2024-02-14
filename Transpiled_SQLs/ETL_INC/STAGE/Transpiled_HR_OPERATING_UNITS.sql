DROP TABLE IF EXISTS bronze_bec_ods_stg.HR_OPERATING_UNITS;
CREATE TABLE bronze_bec_ods_stg.HR_OPERATING_UNITS AS
SELECT
  *
FROM bec_raw_dl_ext.HR_OPERATING_UNITS
WHERE
  kca_operation <> 'DELETE';