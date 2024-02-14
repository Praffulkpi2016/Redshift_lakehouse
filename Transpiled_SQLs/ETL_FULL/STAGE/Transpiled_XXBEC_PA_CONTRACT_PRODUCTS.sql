DROP table IF EXISTS bronze_bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS;
CREATE TABLE bronze_bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.XXBEC_PA_CONTRACT_PRODUCTS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(PRODUCT_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(PRODUCT_ID, 0) AS PRODUCT_ID,
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.XXBEC_PA_CONTRACT_PRODUCTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        PRODUCT_ID,
        INVENTORY_ITEM_ID
    )
);