DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_AGENTS_V;
CREATE TABLE bronze_bec_ods_stg.PO_AGENTS_V AS
SELECT
  *
FROM bec_raw_dl_ext.PO_AGENTS_V
WHERE
  kca_operation <> 'DELETE';