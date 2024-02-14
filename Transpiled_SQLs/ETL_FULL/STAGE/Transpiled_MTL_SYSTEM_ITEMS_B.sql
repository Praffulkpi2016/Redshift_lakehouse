DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_SYSTEM_ITEMS_B;
CREATE TABLE bronze_bec_ods_stg.MTL_SYSTEM_ITEMS_B AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_SYSTEM_ITEMS_B
WHERE
  kca_operation <> 'DELETE'
  AND (INVENTORY_ITEM_ID, ORGANIZATION_ID, last_update_date) IN (
    SELECT
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_SYSTEM_ITEMS_B
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID
  );