/* Delete Records */
DELETE FROM silver_bec_ods.MSC_ITEMS
WHERE
  (
    COALESCE(INVENTORY_ITEM_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID
    FROM silver_bec_ods.MSC_ITEMS AS ods, bronze_bec_ods_stg.MSC_ITEMS AS stg
    WHERE
      COALESCE(ods.INVENTORY_ITEM_ID, 0) = COALESCE(stg.INVENTORY_ITEM_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MSC_ITEMS (
  inventory_item_id,
  item_name,
  description,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    inventory_item_id,
    item_name,
    description,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MSC_ITEMS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(INVENTORY_ITEM_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MSC_ITEMS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(INVENTORY_ITEM_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MSC_ITEMS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MSC_ITEMS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    INVENTORY_ITEM_ID
  ) IN (
    SELECT
      INVENTORY_ITEM_ID
    FROM bec_raw_dl_ext.MSC_ITEMS
    WHERE
      (INVENTORY_ITEM_ID, KCA_SEQ_ID) IN (
        SELECT
          INVENTORY_ITEM_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MSC_ITEMS
        GROUP BY
          INVENTORY_ITEM_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_items';