DROP table IF EXISTS bronze_bec_ods_stg.OKS_LINE_DETAILS_V;
CREATE TABLE bronze_bec_ods_stg.OKS_LINE_DETAILS_V AS
SELECT
  *
FROM bec_raw_dl_ext.OKS_LINE_DETAILS_V
WHERE
  kca_operation <> 'DELETE';