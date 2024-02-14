DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_LOOKUP_CODES;
CREATE TABLE bronze_bec_ods_stg.PO_LOOKUP_CODES AS
SELECT
  *
FROM bec_raw_dl_ext.PO_LOOKUP_CODES
WHERE
  kca_operation <> 'DELETE';