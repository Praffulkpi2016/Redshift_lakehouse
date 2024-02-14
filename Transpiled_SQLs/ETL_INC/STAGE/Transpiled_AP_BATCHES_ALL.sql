TRUNCATE table bronze_bec_ods_stg.AP_BATCHES_ALL;
INSERT INTO bronze_bec_ods_stg.ap_batches_all (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AP_BATCHES_ALL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (batch_id, kca_seq_id) IN (
      SELECT
        batch_id,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.AP_BATCHES_ALL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        batch_id
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_batches_all'
    )
);