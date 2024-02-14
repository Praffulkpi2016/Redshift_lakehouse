DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_LINE_TYPES_VL;
CREATE TABLE bronze_bec_ods_stg.PO_LINE_TYPES_VL AS
SELECT
  *
FROM bec_raw_dl_ext.PO_LINE_TYPES_VL
WHERE
  kca_operation <> 'DELETE';