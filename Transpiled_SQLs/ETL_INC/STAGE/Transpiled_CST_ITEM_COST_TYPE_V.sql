DROP TABLE IF EXISTS bronze_bec_ods_stg.CST_ITEM_COST_TYPE_V;
CREATE TABLE bronze_bec_ods_stg.CST_ITEM_COST_TYPE_V AS
SELECT
  *
FROM bec_raw_dl_ext.CST_ITEM_COST_TYPE_V
WHERE
  kca_operation <> 'DELETE';