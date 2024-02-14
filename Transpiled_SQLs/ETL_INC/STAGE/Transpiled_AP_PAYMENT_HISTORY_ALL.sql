TRUNCATE table
	table bronze_bec_ods_stg.ap_payment_history_all;
INSERT INTO bronze_bec_ods_stg.ap_payment_history_all (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.ap_payment_history_all
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (payment_history_id, KCA_SEQ_ID) IN (
      SELECT
        payment_history_id,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.ap_payment_history_all
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        payment_history_id
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_payment_history_all'
    )
);