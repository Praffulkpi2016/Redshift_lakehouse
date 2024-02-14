DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_SAFETY_STOCKS;
CREATE TABLE bronze_bec_ods_stg.mtl_safety_stocks AS
SELECT
  *
FROM bec_raw_dl_ext.mtl_safety_stocks
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(EFFECTIVITY_DATE, '1900-01-01 12:00:00'), last_update_date) IN (
    SELECT
      COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
      COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
      COALESCE(EFFECTIVITY_DATE, '1900-01-01 12:00:00') AS EFFECTIVITY_DATE,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.mtl_safety_stocks
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(INVENTORY_ITEM_ID, 0),
      COALESCE(ORGANIZATION_ID, 0),
      COALESCE(EFFECTIVITY_DATE, '1900-01-01 12:00:00')
  );