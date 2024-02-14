/* Delete Records */
DELETE FROM silver_bec_ods.ap_payment_hist_dists
WHERE
  payment_hist_dist_id IN (
    SELECT
      stg.payment_hist_dist_id
    FROM silver_bec_ods.ap_payment_hist_dists AS ods, bronze_bec_ods_stg.ap_payment_hist_dists AS stg
    WHERE
      ods.payment_hist_dist_id = stg.payment_hist_dist_id
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ap_payment_hist_dists (
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
  IS_DELETED_FLG,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_payment_hist_dists
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (payment_hist_dist_id, kca_seq_id) IN (
      SELECT
        payment_hist_dist_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.ap_payment_hist_dists
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        payment_hist_dist_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_payment_hist_dists SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_payment_hist_dists SET IS_DELETED_FLG = 'Y'
WHERE
  (
    payment_hist_dist_id
  ) IN (
    SELECT
      payment_hist_dist_id
    FROM bec_raw_dl_ext.ap_payment_hist_dists
    WHERE
      (payment_hist_dist_id, KCA_SEQ_ID) IN (
        SELECT
          payment_hist_dist_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_payment_hist_dists
        GROUP BY
          payment_hist_dist_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_payment_hist_dists';