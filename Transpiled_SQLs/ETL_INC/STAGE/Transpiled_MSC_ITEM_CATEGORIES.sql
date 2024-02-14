TRUNCATE table bronze_bec_ods_stg.MSC_ITEM_CATEGORIES;
INSERT INTO bronze_bec_ods_stg.MSC_ITEM_CATEGORIES (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MSC_ITEM_CATEGORIES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ORGANIZATION_ID, 0), COALESCE(SR_INSTANCE_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(CATEGORY_SET_ID, 0), COALESCE(SR_CATEGORY_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        COALESCE(SR_INSTANCE_ID, 0) AS SR_INSTANCE_ID,
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        COALESCE(CATEGORY_SET_ID, 0) AS CATEGORY_SET_ID,
        COALESCE(SR_CATEGORY_ID, 0) AS SR_CATEGORY_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MSC_ITEM_CATEGORIES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(SR_INSTANCE_ID, 0),
        COALESCE(INVENTORY_ITEM_ID, 0),
        COALESCE(CATEGORY_SET_ID, 0),
        COALESCE(SR_CATEGORY_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'msc_item_categories'
    )
);