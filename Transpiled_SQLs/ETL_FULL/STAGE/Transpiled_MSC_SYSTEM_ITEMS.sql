DROP TABLE IF EXISTS bronze_bec_ods_stg.msc_system_items;
CREATE TABLE bronze_bec_ods_stg.msc_system_items AS
SELECT
  *
FROM bec_raw_dl_ext.msc_system_items
WHERE
  kca_operation <> 'DELETE'
  AND (SR_INSTANCE_ID, PLAN_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, last_update_date) IN (
    SELECT
      SR_INSTANCE_ID,
      PLAN_ID,
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.msc_system_items
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SR_INSTANCE_ID,
      PLAN_ID,
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID
  );