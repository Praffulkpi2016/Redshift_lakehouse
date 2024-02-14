DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_ITEM_SUB_DEFAULTS;
CREATE TABLE bronze_bec_ods_stg.MTL_ITEM_SUB_DEFAULTS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MTL_ITEM_SUB_DEFAULTS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(SUBINVENTORY_CODE, 'NA'), COALESCE(DEFAULT_TYPE, 0), last_update_date) IN (
      SELECT
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        COALESCE(SUBINVENTORY_CODE, 'NA') AS SUBINVENTORY_CODE,
        COALESCE(DEFAULT_TYPE, 0) AS DEFAULT_TYPE,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MTL_ITEM_SUB_DEFAULTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(INVENTORY_ITEM_ID, 0),
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(SUBINVENTORY_CODE, 'NA'),
        COALESCE(DEFAULT_TYPE, 0)
    )
);