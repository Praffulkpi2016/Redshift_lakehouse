TRUNCATE table
	table bronze_bec_ods_stg.ap_payment_hist_dists;
INSERT INTO bronze_bec_ods_stg.ap_payment_hist_dists (
  payment_hist_dist_id,
  accounting_event_id,
  pay_dist_lookup_code,
  invoice_distribution_id,
  amount,
  payment_history_id,
  invoice_payment_id,
  bank_curr_amount,
  cleared_base_amount,
  historical_flag,
  invoice_dist_amount,
  invoice_dist_base_amount,
  invoice_adjustment_event_id,
  matured_base_amount,
  paid_base_amount,
  rounding_amt,
  reversal_flag,
  reversed_pay_hist_dist_id,
  created_by,
  creation_date,
  last_update_date,
  last_updated_by,
  last_update_login,
  program_application_id,
  program_id,
  program_login_id,
  program_update_date,
  request_id,
  awt_related_id,
  release_inv_dist_derived_from,
  pa_addition_flag,
  amount_variance,
  invoice_base_amt_variance,
  quantity_variance,
  invoice_base_qty_variance,
  GAIN_LOSS_INDICATOR,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    payment_hist_dist_id,
    accounting_event_id,
    pay_dist_lookup_code,
    invoice_distribution_id,
    amount,
    payment_history_id,
    invoice_payment_id,
    bank_curr_amount,
    cleared_base_amount,
    historical_flag,
    invoice_dist_amount,
    invoice_dist_base_amount,
    invoice_adjustment_event_id,
    matured_base_amount,
    paid_base_amount,
    rounding_amt,
    reversal_flag,
    reversed_pay_hist_dist_id,
    created_by,
    creation_date,
    last_update_date,
    last_updated_by,
    last_update_login,
    program_application_id,
    program_id,
    program_login_id,
    program_update_date,
    request_id,
    awt_related_id,
    release_inv_dist_derived_from,
    pa_addition_flag,
    amount_variance,
    invoice_base_amt_variance,
    quantity_variance,
    invoice_base_qty_variance,
    GAIN_LOSS_INDICATOR,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.ap_payment_hist_dists
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (payment_hist_dist_id, KCA_SEQ_ID) IN (
      SELECT
        payment_hist_dist_id,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.ap_payment_hist_dists
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        payment_hist_dist_id
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_payment_hist_dists'
    )
);