DROP table IF EXISTS bronze_bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS;
CREATE TABLE bronze_bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_CST_ACTUAL_COST_DETAILS
WHERE
  kca_operation <> 'DELETE'
  AND (LAYER_ID, TRANSACTION_ID, ORGANIZATION_ID, COST_ELEMENT_ID, LEVEL_TYPE, TRANSACTION_ACTION_ID, LAST_UPDATE_DATE) IN (
    SELECT
      LAYER_ID,
      TRANSACTION_ID,
      ORGANIZATION_ID,
      COST_ELEMENT_ID,
      LEVEL_TYPE,
      TRANSACTION_ACTION_ID,
      MAX(LAST_UPDATE_DATE)
    FROM bec_raw_dl_ext.MTL_CST_ACTUAL_COST_DETAILS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      LAYER_ID,
      TRANSACTION_ID,
      ORGANIZATION_ID,
      COST_ELEMENT_ID,
      LEVEL_TYPE,
      TRANSACTION_ACTION_ID
  );