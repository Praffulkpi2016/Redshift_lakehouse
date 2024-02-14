/* Delete Records */
DELETE FROM silver_bec_ods.XXBEC_PA_CONTRACT_PRODUCTS
WHERE
  (COALESCE(PRODUCT_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0)) IN (
    SELECT
      COALESCE(stg.PRODUCT_ID, 0) AS PRODUCT_ID,
      COALESCE(stg.INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID
    FROM silver_bec_ods.XXBEC_PA_CONTRACT_PRODUCTS AS ods, bronze_bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS AS stg
    WHERE
      COALESCE(ods.PRODUCT_ID, 0) = COALESCE(stg.PRODUCT_ID, 0)
      AND COALESCE(ods.INVENTORY_ITEM_ID, 0) = COALESCE(stg.INVENTORY_ITEM_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.XXBEC_PA_CONTRACT_PRODUCTS (
  product_id,
  product_name,
  product_rating,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  inventory_item_id,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    product_id,
    product_name,
    product_rating,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    inventory_item_id,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(PRODUCT_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(PRODUCT_ID, 0) AS PRODUCT_ID,
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(PRODUCT_ID, 0),
        COALESCE(INVENTORY_ITEM_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.XXBEC_PA_CONTRACT_PRODUCTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.XXBEC_PA_CONTRACT_PRODUCTS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(PRODUCT_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0)) IN (
    SELECT
      COALESCE(PRODUCT_ID, 0),
      COALESCE(INVENTORY_ITEM_ID, 0)
    FROM bec_raw_dl_ext.XXBEC_PA_CONTRACT_PRODUCTS
    WHERE
      (COALESCE(PRODUCT_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(PRODUCT_ID, 0),
          COALESCE(INVENTORY_ITEM_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.XXBEC_PA_CONTRACT_PRODUCTS
        GROUP BY
          COALESCE(PRODUCT_ID, 0),
          COALESCE(INVENTORY_ITEM_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'xxbec_pa_contract_products';