DROP TABLE IF EXISTS bronze_bec_ods_stg.MSC_ITEM_CATEGORIES;
CREATE TABLE bronze_bec_ods_stg.MSC_ITEM_CATEGORIES AS
SELECT
  *
FROM bec_raw_dl_ext.MSC_ITEM_CATEGORIES
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ORGANIZATION_ID, 0), COALESCE(SR_INSTANCE_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(CATEGORY_SET_ID, 0), COALESCE(SR_CATEGORY_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
      COALESCE(SR_INSTANCE_ID, 0) AS SR_INSTANCE_ID,
      COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
      COALESCE(CATEGORY_SET_ID, 0) AS CATEGORY_SET_ID,
      COALESCE(SR_CATEGORY_ID, 0) AS SR_CATEGORY_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MSC_ITEM_CATEGORIES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ORGANIZATION_ID, 0),
      COALESCE(SR_INSTANCE_ID, 0),
      COALESCE(INVENTORY_ITEM_ID, 0),
      COALESCE(CATEGORY_SET_ID, 0),
      COALESCE(SR_CATEGORY_ID, 0)
  );