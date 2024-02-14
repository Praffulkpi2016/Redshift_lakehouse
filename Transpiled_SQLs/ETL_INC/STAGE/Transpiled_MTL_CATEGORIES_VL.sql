DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_CATEGORIES_VL;
CREATE TABLE bronze_bec_ods_stg.MTL_CATEGORIES_VL AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_CATEGORIES_VL
WHERE
  kca_operation <> 'DELETE';