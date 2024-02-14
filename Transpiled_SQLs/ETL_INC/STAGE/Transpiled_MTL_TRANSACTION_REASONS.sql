TRUNCATE table bronze_bec_ods_stg.MTL_TRANSACTION_REASONS;
INSERT INTO bronze_bec_ods_stg.MTL_TRANSACTION_REASONS (
  reason_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  reason_name,
  description,
  disable_date,
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
  workflow_name,
  workflow_display_name,
  workflow_process,
  workflow_display_process,
  reason_type,
  reason_type_display,
  reason_context_code,
  kca_operation,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    reason_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    reason_name,
    description,
    disable_date,
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
    workflow_name,
    workflow_display_name,
    workflow_process,
    workflow_display_process,
    reason_type,
    reason_type_display,
    reason_context_code,
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_TRANSACTION_REASONS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (reason_id, kca_seq_id) IN (
      SELECT
        reason_id,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MTL_TRANSACTION_REASONS
      WHERE
        kca_operation <> 'DELETE'
      GROUP BY
        reason_id
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_transaction_reasons'
      )
    )
);