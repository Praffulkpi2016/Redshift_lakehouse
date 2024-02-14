DROP TABLE IF EXISTS bronze_bec_ods_stg.CST_ITEM_COSTS;
CREATE TABLE bronze_bec_ods_stg.CST_ITEM_COSTS AS
SELECT
  *
FROM bec_raw_dl_ext.CST_ITEM_COSTS
WHERE
  kca_operation <> 'DELETE'
  AND (INVENTORY_ITEM_ID, COST_TYPE_ID, ORGANIZATION_ID, last_update_date) IN (
    SELECT
      INVENTORY_ITEM_ID,
      COST_TYPE_ID,
      ORGANIZATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.CST_ITEM_COSTS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      INVENTORY_ITEM_ID,
      COST_TYPE_ID,
      ORGANIZATION_ID
  );