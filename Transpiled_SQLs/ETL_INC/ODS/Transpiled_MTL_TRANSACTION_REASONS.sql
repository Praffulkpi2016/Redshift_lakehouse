/* Delete Records */
DELETE FROM silver_bec_ods.MTL_TRANSACTION_REASONS
WHERE
  (
    reason_id
  ) IN (
    SELECT
      stg.reason_id
    FROM silver_bec_ods.MTL_TRANSACTION_REASONS AS ods, bronze_bec_ods_stg.MTL_TRANSACTION_REASONS AS stg
    WHERE
      ods.reason_id = stg.reason_id AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_TRANSACTION_REASONS (
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
  is_deleted_flg,
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_TRANSACTION_REASONS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (reason_id, kca_seq_id) IN (
      SELECT
        reason_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MTL_TRANSACTION_REASONS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        reason_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_TRANSACTION_REASONS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_TRANSACTION_REASONS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    reason_id
  ) IN (
    SELECT
      reason_id
    FROM bec_raw_dl_ext.MTL_TRANSACTION_REASONS
    WHERE
      (reason_id, KCA_SEQ_ID) IN (
        SELECT
          reason_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_TRANSACTION_REASONS
        GROUP BY
          reason_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_transaction_reasons';