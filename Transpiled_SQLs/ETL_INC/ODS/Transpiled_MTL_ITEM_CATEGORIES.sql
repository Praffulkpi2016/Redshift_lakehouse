/* Delete Records */
DELETE FROM silver_bec_ods.mtl_item_categories
WHERE
  (INVENTORY_ITEM_ID, ORGANIZATION_ID, CATEGORY_SET_ID) IN (
    SELECT
      stg.INVENTORY_ITEM_ID,
      stg.ORGANIZATION_ID,
      stg.CATEGORY_SET_ID
    FROM silver_bec_ods.mtl_item_categories AS ods, bronze_bec_ods_stg.mtl_item_categories AS stg
    WHERE
      ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID
      AND ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
      AND ods.CATEGORY_SET_ID = stg.CATEGORY_SET_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.mtl_item_categories (
  INVENTORY_ITEM_ID,
  ORGANIZATION_ID,
  CATEGORY_SET_ID,
  CATEGORY_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  WH_UPDATE_DATE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    INVENTORY_ITEM_ID,
    ORGANIZATION_ID,
    CATEGORY_SET_ID,
    CATEGORY_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    WH_UPDATE_DATE,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.mtl_item_categories
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (INVENTORY_ITEM_ID, ORGANIZATION_ID, CATEGORY_SET_ID, kca_seq_id) IN (
      SELECT
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        CATEGORY_SET_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.mtl_item_categories
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        CATEGORY_SET_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.mtl_item_categories SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.mtl_item_categories SET IS_DELETED_FLG = 'Y'
WHERE
  (INVENTORY_ITEM_ID, ORGANIZATION_ID, CATEGORY_SET_ID) IN (
    SELECT
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      CATEGORY_SET_ID
    FROM bec_raw_dl_ext.mtl_item_categories
    WHERE
      (INVENTORY_ITEM_ID, ORGANIZATION_ID, CATEGORY_SET_ID, KCA_SEQ_ID) IN (
        SELECT
          INVENTORY_ITEM_ID,
          ORGANIZATION_ID,
          CATEGORY_SET_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.mtl_item_categories
        GROUP BY
          INVENTORY_ITEM_ID,
          ORGANIZATION_ID,
          CATEGORY_SET_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_item_categories';