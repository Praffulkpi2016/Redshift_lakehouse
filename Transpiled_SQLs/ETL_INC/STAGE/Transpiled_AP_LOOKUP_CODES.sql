DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_LOOKUP_CODES;
CREATE TABLE bronze_bec_ods_stg.AP_LOOKUP_CODES AS
SELECT
  *
FROM bec_raw_dl_ext.AP_LOOKUP_CODES
WHERE
  kca_operation <> 'DELETE';