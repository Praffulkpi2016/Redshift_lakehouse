/* Delete Records */
DELETE FROM silver_bec_ods.AR_CASH_RECEIPT_HISTORY_ALL
WHERE
  cash_receipt_history_id IN (
    SELECT
      stg.cash_receipt_history_id
    FROM silver_bec_ods.AR_CASH_RECEIPT_HISTORY_ALL AS ods, bronze_bec_ods_stg.AR_CASH_RECEIPT_HISTORY_ALL AS stg
    WHERE
      ods.cash_receipt_history_id = stg.cash_receipt_history_id
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.AR_CASH_RECEIPT_HISTORY_ALL (
  cash_receipt_history_id,
  cash_receipt_id,
  status,
  trx_date,
  amount,
  first_posted_record_flag,
  postable_flag,
  factor_flag,
  gl_date,
  current_record_flag,
  batch_id,
  account_code_combination_id,
  reversal_gl_date,
  reversal_cash_receipt_hist_id,
  factor_discount_amount,
  bank_charge_account_ccid,
  posting_control_id,
  reversal_posting_control_id,
  gl_posted_date,
  reversal_gl_posted_date,
  last_update_login,
  acctd_amount,
  acctd_factor_discount_amount,
  created_by,
  creation_date,
  exchange_date,
  exchange_rate,
  exchange_rate_type,
  last_update_date,
  program_application_id,
  program_id,
  program_update_date,
  request_id,
  last_updated_by,
  prv_stat_cash_receipt_hist_id,
  created_from,
  reversal_created_from,
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
  org_id,
  MRC_POSTING_CONTROL_ID,
  note_status,
  mrc_gl_posted_date,
  mrc_reversal_gl_posted_date,
  mrc_acctd_amount,
  mrc_acctd_factor_disc_amount,
  mrc_exchange_date,
  mrc_exchange_rate,
  mrc_exchange_rate_type,
  event_id,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    cash_receipt_history_id,
    cash_receipt_id,
    status,
    trx_date,
    amount,
    first_posted_record_flag,
    postable_flag,
    factor_flag,
    gl_date,
    current_record_flag,
    batch_id,
    account_code_combination_id,
    reversal_gl_date,
    reversal_cash_receipt_hist_id,
    factor_discount_amount,
    bank_charge_account_ccid,
    posting_control_id,
    reversal_posting_control_id,
    gl_posted_date,
    reversal_gl_posted_date,
    last_update_login,
    acctd_amount,
    acctd_factor_discount_amount,
    created_by,
    creation_date,
    exchange_date,
    exchange_rate,
    exchange_rate_type,
    last_update_date,
    program_application_id,
    program_id,
    program_update_date,
    request_id,
    last_updated_by,
    prv_stat_cash_receipt_hist_id,
    created_from,
    reversal_created_from,
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
    org_id,
    MRC_POSTING_CONTROL_ID,
    note_status,
    mrc_gl_posted_date,
    mrc_reversal_gl_posted_date,
    mrc_acctd_amount,
    mrc_acctd_factor_disc_amount,
    mrc_exchange_date,
    mrc_exchange_rate,
    mrc_exchange_rate_type,
    event_id,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.AR_CASH_RECEIPT_HISTORY_ALL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (cash_receipt_history_id, kca_seq_id) IN (
      SELECT
        cash_receipt_history_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.AR_CASH_RECEIPT_HISTORY_ALL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        cash_receipt_history_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.AR_CASH_RECEIPT_HISTORY_ALL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.AR_CASH_RECEIPT_HISTORY_ALL SET IS_DELETED_FLG = 'Y'
WHERE
  (
    cash_receipt_history_id
  ) IN (
    SELECT
      cash_receipt_history_id
    FROM bec_raw_dl_ext.AR_CASH_RECEIPT_HISTORY_ALL
    WHERE
      (cash_receipt_history_id, KCA_SEQ_ID) IN (
        SELECT
          cash_receipt_history_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.AR_CASH_RECEIPT_HISTORY_ALL
        GROUP BY
          cash_receipt_history_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ar_cash_receipt_history_all';