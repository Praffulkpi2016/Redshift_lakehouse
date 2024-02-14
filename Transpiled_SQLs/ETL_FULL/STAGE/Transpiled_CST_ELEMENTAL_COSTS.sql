DROP TABLE IF EXISTS bronze_bec_ods_stg.CST_ELEMENTAL_COSTS;
CREATE TABLE bronze_bec_ods_stg.CST_ELEMENTAL_COSTS AS
SELECT
  *
FROM bec_raw_dl_ext.CST_ELEMENTAL_COSTS
WHERE
  kca_operation <> 'DELETE'
  AND (COST_UPDATE_ID, ORGANIZATION_ID, INVENTORY_ITEM_ID, COST_ELEMENT_ID, last_update_date) IN (
    SELECT
      COST_UPDATE_ID,
      ORGANIZATION_ID,
      INVENTORY_ITEM_ID,
      COST_ELEMENT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.CST_ELEMENTAL_COSTS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COST_UPDATE_ID,
      ORGANIZATION_ID,
      INVENTORY_ITEM_ID,
      COST_ELEMENT_ID
  );