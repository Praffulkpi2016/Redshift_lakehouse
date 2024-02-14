DROP table IF EXISTS bronze_bec_ods_stg.FND_ID_FLEX_STRUCTURES_VL;
CREATE TABLE bronze_bec_ods_stg.FND_ID_FLEX_STRUCTURES_VL AS
SELECT
  *
FROM bec_raw_dl_ext.FND_ID_FLEX_STRUCTURES_VL
WHERE
  kca_operation <> 'DELETE';