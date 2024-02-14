DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_PHYSICAL_INVENTORIES;
CREATE TABLE bronze_bec_ods_stg.MTL_PHYSICAL_INVENTORIES AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MTL_PHYSICAL_INVENTORIES
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(ORGANIZATION_ID, 0), COALESCE(PHYSICAL_INVENTORY_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(PHYSICAL_INVENTORY_ID, 0),
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MTL_PHYSICAL_INVENTORIES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(PHYSICAL_INVENTORY_ID, 0)
    )
);