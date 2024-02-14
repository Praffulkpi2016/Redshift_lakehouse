TRUNCATE table bronze_bec_ods_stg.MTL_PHYSICAL_INVENTORIES;
INSERT INTO bronze_bec_ods_stg.MTL_PHYSICAL_INVENTORIES (
  physical_inventory_id,
  organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  physical_inventory_date,
  last_adjustment_date,
  total_adjustment_value,
  description,
  freeze_date,
  physical_inventory_name,
  approval_required,
  all_subinventories_flag,
  next_tag_number,
  tag_number_increments,
  default_gl_adjust_account,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  approval_tolerance_pos,
  approval_tolerance_neg,
  cost_variance_pos,
  cost_variance_neg,
  number_of_skus,
  dynamic_tag_entry_flag,
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
  attribute_category,
  exclude_zero_balance,
  exclude_negative_balance,
  KCA_OPERATION,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    physical_inventory_id,
    organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    physical_inventory_date,
    last_adjustment_date,
    total_adjustment_value,
    description,
    freeze_date,
    physical_inventory_name,
    approval_required,
    all_subinventories_flag,
    next_tag_number,
    tag_number_increments,
    default_gl_adjust_account,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    approval_tolerance_pos,
    approval_tolerance_neg,
    cost_variance_pos,
    cost_variance_neg,
    number_of_skus,
    dynamic_tag_entry_flag,
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
    attribute_category,
    exclude_zero_balance,
    exclude_negative_balance,
    KCA_OPERATION,
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_PHYSICAL_INVENTORIES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ORGANIZATION_ID, 0), COALESCE(PHYSICAL_INVENTORY_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        COALESCE(PHYSICAL_INVENTORY_ID, 0) AS PHYSICAL_INVENTORY_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.MTL_PHYSICAL_INVENTORIES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(PHYSICAL_INVENTORY_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_physical_inventories'
      )
    )
);