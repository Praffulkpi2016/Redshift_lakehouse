/* Delete Records */
DELETE FROM silver_bec_ods.AR_ADJUSTMENTS_ALL
WHERE
  adjustment_id IN (
    SELECT
      stg.adjustment_id
    FROM silver_bec_ods.AR_ADJUSTMENTS_ALL AS ods, bronze_bec_ods_stg.AR_ADJUSTMENTS_ALL AS stg
    WHERE
      ods.adjustment_id = stg.adjustment_id AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.AR_ADJUSTMENTS_ALL (
  adjustment_id,
  last_updated_by,
  last_update_date,
  last_update_login,
  created_by,
  creation_date,
  amount,
  apply_date,
  gl_date,
  set_of_books_id,
  code_combination_id,
  `type`,
  adjustment_type,
  status,
  line_adjusted,
  freight_adjusted,
  tax_adjusted,
  receivables_charges_adjusted,
  associated_cash_receipt_id,
  chargeback_customer_trx_id,
  batch_id,
  customer_trx_id,
  customer_trx_line_id,
  subsequent_trx_id,
  payment_schedule_id,
  receivables_trx_id,
  distribution_set_id,
  gl_posted_date,
  comments,
  automatically_generated,
  created_from,
  reason_code,
  postable,
  approved_by,
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
  posting_control_id,
  acctd_amount,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  program_application_id,
  program_id,
  program_update_date,
  request_id,
  adjustment_number,
  org_id,
  ussgl_transaction_code,
  ussgl_transaction_code_context,
  doc_sequence_value,
  doc_sequence_id,
  associated_application_id,
  cons_inv_id,
  mrc_gl_posted_date,
  mrc_posting_control_id,
  mrc_acctd_amount,
  adj_tax_acct_rule,
  global_attribute_category,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  link_to_trx_hist_id,
  event_id,
  upgrade_method,
  ax_accounted_flag,
  interest_header_id,
  interest_line_id,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    adjustment_id,
    last_updated_by,
    last_update_date,
    last_update_login,
    created_by,
    creation_date,
    amount,
    apply_date,
    gl_date,
    set_of_books_id,
    code_combination_id,
    `type`,
    adjustment_type,
    status,
    line_adjusted,
    freight_adjusted,
    tax_adjusted,
    receivables_charges_adjusted,
    associated_cash_receipt_id,
    chargeback_customer_trx_id,
    batch_id,
    customer_trx_id,
    customer_trx_line_id,
    subsequent_trx_id,
    payment_schedule_id,
    receivables_trx_id,
    distribution_set_id,
    gl_posted_date,
    comments,
    automatically_generated,
    created_from,
    reason_code,
    postable,
    approved_by,
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
    posting_control_id,
    acctd_amount,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    program_application_id,
    program_id,
    program_update_date,
    request_id,
    adjustment_number,
    org_id,
    ussgl_transaction_code,
    ussgl_transaction_code_context,
    doc_sequence_value,
    doc_sequence_id,
    associated_application_id,
    cons_inv_id,
    mrc_gl_posted_date,
    mrc_posting_control_id,
    mrc_acctd_amount,
    adj_tax_acct_rule,
    global_attribute_category,
    global_attribute1,
    global_attribute2,
    global_attribute3,
    global_attribute4,
    global_attribute5,
    global_attribute6,
    global_attribute7,
    global_attribute8,
    global_attribute9,
    global_attribute10,
    global_attribute11,
    global_attribute12,
    global_attribute13,
    global_attribute14,
    global_attribute15,
    global_attribute16,
    global_attribute17,
    global_attribute18,
    global_attribute19,
    global_attribute20,
    link_to_trx_hist_id,
    event_id,
    upgrade_method,
    ax_accounted_flag,
    interest_header_id,
    interest_line_id,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.AR_ADJUSTMENTS_ALL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (adjustment_id, kca_seq_id) IN (
      SELECT
        adjustment_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.AR_ADJUSTMENTS_ALL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        adjustment_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.AR_ADJUSTMENTS_ALL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.AR_ADJUSTMENTS_ALL SET IS_DELETED_FLG = 'Y'
WHERE
  (
    adjustment_id
  ) IN (
    SELECT
      adjustment_id
    FROM bec_raw_dl_ext.AR_ADJUSTMENTS_ALL
    WHERE
      (adjustment_id, KCA_SEQ_ID) IN (
        SELECT
          adjustment_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.AR_ADJUSTMENTS_ALL
        GROUP BY
          adjustment_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ar_adjustments_all';