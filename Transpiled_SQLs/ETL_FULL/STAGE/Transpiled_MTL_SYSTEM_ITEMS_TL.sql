DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_SYSTEM_ITEMS_TL;
CREATE TABLE bronze_bec_ods_stg.MTL_SYSTEM_ITEMS_TL AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MTL_SYSTEM_ITEMS_TL
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(LANGUAGE, ''), last_update_date) IN (
      SELECT
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        COALESCE(LANGUAGE, '') AS LANGUAGE,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MTL_SYSTEM_ITEMS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(INVENTORY_ITEM_ID, 0),
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(LANGUAGE, '')
    )
);