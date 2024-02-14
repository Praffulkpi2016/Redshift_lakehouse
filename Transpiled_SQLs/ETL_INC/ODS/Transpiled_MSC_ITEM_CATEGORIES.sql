/* Delete Records */
DELETE FROM silver_bec_ods.MSC_ITEM_CATEGORIES
WHERE
  (COALESCE(ORGANIZATION_ID, 0), COALESCE(SR_INSTANCE_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(CATEGORY_SET_ID, 0), COALESCE(SR_CATEGORY_ID, 0)) IN (
    SELECT
      COALESCE(stg.ORGANIZATION_ID, 0),
      COALESCE(stg.SR_INSTANCE_ID, 0),
      COALESCE(stg.INVENTORY_ITEM_ID, 0),
      COALESCE(stg.CATEGORY_SET_ID, 0),
      COALESCE(stg.SR_CATEGORY_ID, 0)
    FROM silver_bec_ods.MSC_ITEM_CATEGORIES AS ods, bronze_bec_ods_stg.MSC_ITEM_CATEGORIES AS stg
    WHERE
      ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
      AND ods.SR_INSTANCE_ID = stg.SR_INSTANCE_ID
      AND ods.CATEGORY_SET_ID = stg.CATEGORY_SET_ID
      AND ods.SR_CATEGORY_ID = stg.SR_CATEGORY_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MSC_ITEM_CATEGORIES (
  organization_id,
  inventory_item_id,
  category_set_id,
  category_name,
  description,
  disable_date,
  summary_flag,
  enabled_flag,
  start_date_active,
  end_date_active,
  sr_instance_id,
  sr_category_id,
  refresh_number,
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
    organization_id,
    inventory_item_id,
    category_set_id,
    category_name,
    description,
    disable_date,
    summary_flag,
    enabled_flag,
    start_date_active,
    end_date_active,
    sr_instance_id,
    sr_category_id,
    refresh_number,
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MSC_ITEM_CATEGORIES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ORGANIZATION_ID, 0), COALESCE(SR_INSTANCE_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(CATEGORY_SET_ID, 0), COALESCE(SR_CATEGORY_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        COALESCE(SR_INSTANCE_ID, 0) AS SR_INSTANCE_ID,
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        COALESCE(CATEGORY_SET_ID, 0) AS CATEGORY_SET_ID,
        COALESCE(SR_CATEGORY_ID, 0) AS SR_CATEGORY_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MSC_ITEM_CATEGORIES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(SR_INSTANCE_ID, 0),
        COALESCE(INVENTORY_ITEM_ID, 0),
        COALESCE(CATEGORY_SET_ID, 0),
        COALESCE(SR_CATEGORY_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MSC_ITEM_CATEGORIES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MSC_ITEM_CATEGORIES SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(ORGANIZATION_ID, 0), COALESCE(SR_INSTANCE_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(CATEGORY_SET_ID, 0), COALESCE(SR_CATEGORY_ID, 0)) IN (
    SELECT
      COALESCE(ORGANIZATION_ID, 0),
      COALESCE(SR_INSTANCE_ID, 0),
      COALESCE(INVENTORY_ITEM_ID, 0),
      COALESCE(CATEGORY_SET_ID, 0),
      COALESCE(SR_CATEGORY_ID, 0)
    FROM bec_raw_dl_ext.MSC_ITEM_CATEGORIES
    WHERE
      (COALESCE(ORGANIZATION_ID, 0), COALESCE(SR_INSTANCE_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(CATEGORY_SET_ID, 0), COALESCE(SR_CATEGORY_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(ORGANIZATION_ID, 0),
          COALESCE(SR_INSTANCE_ID, 0),
          COALESCE(INVENTORY_ITEM_ID, 0),
          COALESCE(CATEGORY_SET_ID, 0),
          COALESCE(SR_CATEGORY_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MSC_ITEM_CATEGORIES
        GROUP BY
          COALESCE(ORGANIZATION_ID, 0),
          COALESCE(SR_INSTANCE_ID, 0),
          COALESCE(INVENTORY_ITEM_ID, 0),
          COALESCE(CATEGORY_SET_ID, 0),
          COALESCE(SR_CATEGORY_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_item_categories';