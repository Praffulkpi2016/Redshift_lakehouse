DROP TABLE IF EXISTS bronze_bec_ods_stg.BEC_ACTUAL_PO_RECPT1;
CREATE TABLE bronze_bec_ods_stg.BEC_ACTUAL_PO_RECPT1 AS
SELECT
  *
FROM bec_raw_dl_ext.BEC_ACTUAL_PO_RECPT1
WHERE
  kca_operation <> 'DELETE';