TRUNCATE table bronze_bec_ods_stg.ap_invoice_payments_all;
INSERT INTO bronze_bec_ods_stg.ap_invoice_payments_all (
  discount_lost,
  discount_taken,
  exchange_date,
  exchange_rate,
  exchange_rate_type,
  gain_code_combination_id,
  invoice_base_amount,
  loss_code_combination_id,
  payment_base_amount,
  attribute1,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute_category,
  cash_je_batch_id,
  future_pay_code_combination_id,
  future_pay_posted_flag,
  je_batch_id,
  electronic_transfer_id,
  assets_addition_flag,
  invoice_payment_type,
  other_invoice_id,
  org_id,
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
  external_bank_account_id,
  accounting_event_id,
  mrc_exchange_date,
  mrc_exchange_rate,
  mrc_exchange_rate_type,
  mrc_gain_code_combination_id,
  mrc_invoice_base_amount,
  mrc_loss_code_combination_id,
  mrc_payment_base_amount,
  reversal_flag,
  reversal_inv_pmt_id,
  iban_number,
  invoicing_party_id,
  invoicing_party_site_id,
  invoicing_vendor_site_id,
  remit_to_supplier_name,
  remit_to_supplier_id,
  remit_to_supplier_site,
  remit_to_supplier_site_id,
  accounting_date,
  accrual_posted_flag,
  amount,
  cash_posted_flag,
  check_id,
  invoice_id,
  invoice_payment_id,
  last_updated_by,
  last_update_date,
  payment_num,
  period_name,
  posted_flag,
  set_of_books_id,
  accts_pay_code_combination_id,
  asset_code_combination_id,
  created_by,
  creation_date,
  last_update_login,
  bank_account_num,
  bank_account_type,
  bank_num,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    discount_lost,
    discount_taken,
    exchange_date,
    exchange_rate,
    exchange_rate_type,
    gain_code_combination_id,
    invoice_base_amount,
    loss_code_combination_id,
    payment_base_amount,
    attribute1,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute_category,
    cash_je_batch_id,
    future_pay_code_combination_id,
    future_pay_posted_flag,
    je_batch_id,
    electronic_transfer_id,
    assets_addition_flag,
    invoice_payment_type,
    other_invoice_id,
    org_id,
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
    external_bank_account_id,
    accounting_event_id,
    mrc_exchange_date,
    mrc_exchange_rate,
    mrc_exchange_rate_type,
    mrc_gain_code_combination_id,
    mrc_invoice_base_amount,
    mrc_loss_code_combination_id,
    mrc_payment_base_amount,
    reversal_flag,
    reversal_inv_pmt_id,
    iban_number,
    invoicing_party_id,
    invoicing_party_site_id,
    invoicing_vendor_site_id,
    remit_to_supplier_name,
    remit_to_supplier_id,
    remit_to_supplier_site,
    remit_to_supplier_site_id,
    accounting_date,
    accrual_posted_flag,
    amount,
    cash_posted_flag,
    check_id,
    invoice_id,
    invoice_payment_id,
    last_updated_by,
    last_update_date,
    payment_num,
    period_name,
    posted_flag,
    set_of_books_id,
    accts_pay_code_combination_id,
    asset_code_combination_id,
    created_by,
    creation_date,
    last_update_login,
    bank_account_num,
    bank_account_type,
    bank_num,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.ap_invoice_payments_all
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (INVOICE_PAYMENT_ID, KCA_SEQ_ID) IN (
      SELECT
        INVOICE_PAYMENT_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.ap_invoice_payments_all
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        INVOICE_PAYMENT_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_invoice_payments_all'
    )
);