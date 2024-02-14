DROP TABLE IF EXISTS bronze_bec_ods_stg.MSC_ITEMS;
CREATE TABLE bronze_bec_ods_stg.MSC_ITEMS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MSC_ITEMS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(INVENTORY_ITEM_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MSC_ITEMS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(INVENTORY_ITEM_ID, 0)
    )
);