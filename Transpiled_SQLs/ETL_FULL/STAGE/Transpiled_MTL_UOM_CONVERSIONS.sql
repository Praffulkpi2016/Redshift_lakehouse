DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_UOM_CONVERSIONS;
CREATE TABLE bronze_bec_ods_stg.MTL_UOM_CONVERSIONS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_UOM_CONVERSIONS
WHERE
  kca_operation <> 'DELETE'
  AND (INVENTORY_ITEM_ID, UNIT_OF_MEASURE, last_update_date) IN (
    SELECT
      INVENTORY_ITEM_ID,
      UNIT_OF_MEASURE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_UOM_CONVERSIONS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      INVENTORY_ITEM_ID,
      UNIT_OF_MEASURE
  );