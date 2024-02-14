DROP TABLE IF EXISTS bronze_bec_ods_stg.MSC_VMI_ITEM_V;
CREATE TABLE bronze_bec_ods_stg.MSC_VMI_ITEM_V AS
SELECT
  *
FROM bec_raw_dl_ext.MSC_VMI_ITEM_V
WHERE
  kca_operation <> 'DELETE';