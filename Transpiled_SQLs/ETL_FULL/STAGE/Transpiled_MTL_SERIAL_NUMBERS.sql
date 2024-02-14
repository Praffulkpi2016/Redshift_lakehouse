DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_SERIAL_NUMBERS;
CREATE TABLE bronze_bec_ods_stg.MTL_SERIAL_NUMBERS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_SERIAL_NUMBERS
WHERE
  kca_operation <> 'DELETE'
  AND (SERIAL_NUMBER, INVENTORY_ITEM_ID, CURRENT_ORGANIZATION_ID, last_update_date) IN (
    SELECT
      SERIAL_NUMBER,
      INVENTORY_ITEM_ID,
      CURRENT_ORGANIZATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_SERIAL_NUMBERS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SERIAL_NUMBER,
      INVENTORY_ITEM_ID,
      CURRENT_ORGANIZATION_ID
  );