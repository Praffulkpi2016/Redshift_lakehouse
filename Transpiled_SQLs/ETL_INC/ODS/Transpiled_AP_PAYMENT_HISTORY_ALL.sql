/* Delete Records */
DELETE FROM silver_bec_ods.ap_payment_history_all
WHERE
  payment_history_id IN (
    SELECT
      stg.payment_history_id
    FROM silver_bec_ods.ap_payment_history_all AS ods, bronze_bec_ods_stg.ap_payment_history_all AS stg
    WHERE
      ods.payment_history_id = stg.payment_history_id
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ap_payment_history_all (
  payment_history_id,
  check_id,
  accounting_date,
  transaction_type,
  posted_flag,
  matched_flag,
  accounting_event_id,
  org_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  program_update_date,
  program_application_id,
  program_id,
  request_id,
  rev_pmt_hist_id,
  trx_bank_amount,
  errors_bank_amount,
  charges_bank_amount,
  trx_pmt_amount,
  errors_pmt_amount,
  charges_pmt_amount,
  trx_base_amount,
  errors_base_amount,
  charges_base_amount,
  bank_currency_code,
  bank_to_base_xrate_type,
  bank_to_base_xrate_date,
  bank_to_base_xrate,
  pmt_currency_code,
  pmt_to_base_xrate_type,
  pmt_to_base_xrate_date,
  pmt_to_base_xrate,
  mrc_pmt_to_base_xrate_type,
  mrc_pmt_to_base_xrate_date,
  mrc_pmt_to_base_xrate,
  mrc_bank_to_base_xrate_type,
  mrc_bank_to_base_xrate_date,
  mrc_bank_to_base_xrate,
  mrc_trx_base_amount,
  mrc_errors_base_amount,
  mrc_charges_base_amount,
  related_event_id,
  historical_flag,
  invoice_adjustment_event_id,
  gain_loss_indicator,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    payment_history_id,
    check_id,
    accounting_date,
    transaction_type,
    posted_flag,
    matched_flag,
    accounting_event_id,
    org_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    program_update_date,
    program_application_id,
    program_id,
    request_id,
    rev_pmt_hist_id,
    trx_bank_amount,
    errors_bank_amount,
    charges_bank_amount,
    trx_pmt_amount,
    errors_pmt_amount,
    charges_pmt_amount,
    trx_base_amount,
    errors_base_amount,
    charges_base_amount,
    bank_currency_code,
    bank_to_base_xrate_type,
    bank_to_base_xrate_date,
    bank_to_base_xrate,
    pmt_currency_code,
    pmt_to_base_xrate_type,
    pmt_to_base_xrate_date,
    pmt_to_base_xrate,
    mrc_pmt_to_base_xrate_type,
    mrc_pmt_to_base_xrate_date,
    mrc_pmt_to_base_xrate,
    mrc_bank_to_base_xrate_type,
    mrc_bank_to_base_xrate_date,
    mrc_bank_to_base_xrate,
    mrc_trx_base_amount,
    mrc_errors_base_amount,
    mrc_charges_base_amount,
    related_event_id,
    historical_flag,
    invoice_adjustment_event_id,
    gain_loss_indicator,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_payment_history_all
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (payment_history_id, KCA_SEQ_ID) IN (
      SELECT
        payment_history_id,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.ap_payment_history_all
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        payment_history_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_payment_history_all SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_payment_history_all SET IS_DELETED_FLG = 'Y'
WHERE
  (
    payment_history_id
  ) IN (
    SELECT
      payment_history_id
    FROM bec_raw_dl_ext.ap_payment_history_all
    WHERE
      (payment_history_id, KCA_SEQ_ID) IN (
        SELECT
          payment_history_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_payment_history_all
        GROUP BY
          payment_history_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_payment_history_all';