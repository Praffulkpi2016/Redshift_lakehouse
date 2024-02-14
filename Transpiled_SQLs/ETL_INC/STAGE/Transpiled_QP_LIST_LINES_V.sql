DROP table IF EXISTS bronze_bec_ods_stg.QP_LIST_LINES_V;
CREATE TABLE bronze_bec_ods_stg.QP_LIST_LINES_V AS
SELECT
  *
FROM bec_raw_dl_ext.QP_LIST_LINES_V
WHERE
  kca_operation <> 'DELETE';