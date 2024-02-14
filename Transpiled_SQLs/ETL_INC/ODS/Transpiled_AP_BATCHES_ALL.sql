/* Delete Records */
DELETE FROM silver_bec_ods.ap_batches_all
WHERE
  batch_id IN (
    SELECT
      stg.batch_id
    FROM silver_bec_ods.ap_batches_all AS ods, bronze_bec_ods_stg.ap_batches_all AS stg
    WHERE
      ods.batch_id = stg.batch_id AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ap_batches_all (
  batch_id,
  batch_name,
  batch_date,
  last_update_date,
  last_updated_by,
  control_invoice_count,
  control_invoice_total,
  actual_invoice_count,
  actual_invoice_total,
  invoice_currency_code,
  payment_currency_code,
  last_update_login,
  creation_date,
  created_by,
  pay_group_lookup_code,
  batch_code_combination_id,
  terms_id,
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
  invoice_type_lookup_code,
  hold_lookup_code,
  hold_reason,
  doc_category_code,
  org_id,
  gl_date,
  payment_priority,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    batch_id,
    batch_name,
    batch_date,
    last_update_date,
    last_updated_by,
    control_invoice_count,
    control_invoice_total,
    actual_invoice_count,
    actual_invoice_total,
    invoice_currency_code,
    payment_currency_code,
    last_update_login,
    creation_date,
    created_by,
    pay_group_lookup_code,
    batch_code_combination_id,
    terms_id,
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
    invoice_type_lookup_code,
    hold_lookup_code,
    hold_reason,
    doc_category_code,
    org_id,
    gl_date,
    payment_priority,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_batches_all
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (batch_id, kca_seq_id) IN (
      SELECT
        batch_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.ap_batches_all
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        batch_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_batches_all SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_batches_all SET IS_DELETED_FLG = 'Y'
WHERE
  (
    batch_id
  ) IN (
    SELECT
      batch_id
    FROM bec_raw_dl_ext.ap_batches_all
    WHERE
      (batch_id, KCA_SEQ_ID) IN (
        SELECT
          batch_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_batches_all
        GROUP BY
          batch_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_batches_all';