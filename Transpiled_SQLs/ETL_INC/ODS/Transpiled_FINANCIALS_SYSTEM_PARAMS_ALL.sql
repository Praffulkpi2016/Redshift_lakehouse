/* Delete Records */
DELETE FROM silver_bec_ods.FINANCIALS_SYSTEM_PARAMS_ALL
WHERE
  (ORG_ID, SET_OF_BOOKS_ID) IN (
    SELECT
      stg.ORG_ID,
      stg.SET_OF_BOOKS_ID
    FROM silver_bec_ods.FINANCIALS_SYSTEM_PARAMS_ALL AS ods, bronze_bec_ods_stg.FINANCIALS_SYSTEM_PARAMS_ALL AS stg
    WHERE
      ods.ORG_ID = stg.ORG_ID
      AND ods.SET_OF_BOOKS_ID = stg.SET_OF_BOOKS_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FINANCIALS_SYSTEM_PARAMS_ALL (
  last_update_date,
  last_updated_by,
  set_of_books_id,
  payment_method_lookup_code,
  user_defined_vendor_num_code,
  vendor_num_start_num,
  ship_to_location_id,
  bill_to_location_id,
  ship_via_lookup_code,
  fob_lookup_code,
  terms_id,
  always_take_disc_flag,
  pay_date_basis_lookup_code,
  invoice_currency_code,
  payment_currency_code,
  accts_pay_code_combination_id,
  prepay_code_combination_id,
  disc_taken_code_combination_id,
  future_period_limit,
  reserve_at_completion_flag,
  res_encumb_code_combination_id,
  req_encumbrance_flag,
  req_encumbrance_type_id,
  purch_encumbrance_flag,
  purch_encumbrance_type_id,
  inv_encumbrance_type_id,
  manual_vendor_num_type,
  inventory_organization_id,
  last_update_login,
  creation_date,
  created_by,
  freight_terms_lookup_code,
  rfq_only_site_flag,
  receipt_acceptance_days,
  business_group_id,
  expense_check_address_flag,
  terms_date_basis,
  use_positions_flag,
  rate_var_code_combination_id,
  hold_unmatched_invoices_flag,
  exclusive_payment_flag,
  revision_sort_ordering,
  vat_registration_num,
  vat_country_code,
  rate_var_gain_ccid,
  rate_var_loss_ccid,
  org_id,
  bank_charge_bearer,
  vat_code,
  match_option,
  non_recoverable_tax_flag,
  tax_rounding_rule,
  `precision`,
  minimum_accountable_unit,
  default_recovery_rate,
  cash_basis_enc_nr_tax,
  future_dated_payment_ccid,
  expense_clearing_ccid,
  misc_charge_ccid,
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
  retainage_code_combination_id,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    last_update_date,
    last_updated_by,
    set_of_books_id,
    payment_method_lookup_code,
    user_defined_vendor_num_code,
    vendor_num_start_num,
    ship_to_location_id,
    bill_to_location_id,
    ship_via_lookup_code,
    fob_lookup_code,
    terms_id,
    always_take_disc_flag,
    pay_date_basis_lookup_code,
    invoice_currency_code,
    payment_currency_code,
    accts_pay_code_combination_id,
    prepay_code_combination_id,
    disc_taken_code_combination_id,
    future_period_limit,
    reserve_at_completion_flag,
    res_encumb_code_combination_id,
    req_encumbrance_flag,
    req_encumbrance_type_id,
    purch_encumbrance_flag,
    purch_encumbrance_type_id,
    inv_encumbrance_type_id,
    manual_vendor_num_type,
    inventory_organization_id,
    last_update_login,
    creation_date,
    created_by,
    freight_terms_lookup_code,
    rfq_only_site_flag,
    receipt_acceptance_days,
    business_group_id,
    expense_check_address_flag,
    terms_date_basis,
    use_positions_flag,
    rate_var_code_combination_id,
    hold_unmatched_invoices_flag,
    exclusive_payment_flag,
    revision_sort_ordering,
    vat_registration_num,
    vat_country_code,
    rate_var_gain_ccid,
    rate_var_loss_ccid,
    org_id,
    bank_charge_bearer,
    vat_code,
    match_option,
    non_recoverable_tax_flag,
    tax_rounding_rule,
    `precision`,
    minimum_accountable_unit,
    default_recovery_rate,
    cash_basis_enc_nr_tax,
    future_dated_payment_ccid,
    expense_clearing_ccid,
    misc_charge_ccid,
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
    retainage_code_combination_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FINANCIALS_SYSTEM_PARAMS_ALL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (ORG_ID, SET_OF_BOOKS_ID, kca_seq_id) IN (
      SELECT
        ORG_ID,
        SET_OF_BOOKS_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.FINANCIALS_SYSTEM_PARAMS_ALL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        ORG_ID,
        SET_OF_BOOKS_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FINANCIALS_SYSTEM_PARAMS_ALL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FINANCIALS_SYSTEM_PARAMS_ALL SET IS_DELETED_FLG = 'Y'
WHERE
  (ORG_ID, SET_OF_BOOKS_ID) IN (
    SELECT
      ORG_ID,
      SET_OF_BOOKS_ID
    FROM bec_raw_dl_ext.FINANCIALS_SYSTEM_PARAMS_ALL
    WHERE
      (ORG_ID, SET_OF_BOOKS_ID, KCA_SEQ_ID) IN (
        SELECT
          ORG_ID,
          SET_OF_BOOKS_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FINANCIALS_SYSTEM_PARAMS_ALL
        GROUP BY
          ORG_ID,
          SET_OF_BOOKS_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'financials_system_params_all';