DROP table IF EXISTS silver_bec_ods.ap_invoice_payments_all;
CREATE TABLE IF NOT EXISTS silver_bec_ods.ap_invoice_payments_all (
  discount_lost DECIMAL(28, 10),
  discount_taken DECIMAL(28, 10),
  exchange_date TIMESTAMP,
  exchange_rate DECIMAL(28, 10),
  exchange_rate_type STRING,
  gain_code_combination_id DECIMAL(15, 0),
  invoice_base_amount DECIMAL(28, 10),
  loss_code_combination_id DECIMAL(15, 0),
  payment_base_amount DECIMAL(28, 10),
  attribute1 STRING,
  attribute10 STRING,
  attribute11 STRING,
  attribute12 STRING,
  attribute13 STRING,
  attribute14 STRING,
  attribute15 STRING,
  attribute2 STRING,
  attribute3 STRING,
  attribute4 STRING,
  attribute5 STRING,
  attribute6 STRING,
  attribute7 STRING,
  attribute8 STRING,
  attribute9 STRING,
  attribute_category STRING,
  cash_je_batch_id DECIMAL(15, 0),
  future_pay_code_combination_id DECIMAL(15, 0),
  future_pay_posted_flag STRING,
  je_batch_id DECIMAL(15, 0),
  electronic_transfer_id DECIMAL(15, 0),
  assets_addition_flag STRING,
  invoice_payment_type STRING,
  other_invoice_id DECIMAL(15, 0),
  org_id DECIMAL(15, 0),
  global_attribute_category STRING,
  global_attribute1 STRING,
  global_attribute2 STRING,
  global_attribute3 STRING,
  global_attribute4 STRING,
  global_attribute5 STRING,
  global_attribute6 STRING,
  global_attribute7 STRING,
  global_attribute8 STRING,
  global_attribute9 STRING,
  global_attribute10 STRING,
  global_attribute11 STRING,
  global_attribute12 STRING,
  global_attribute13 STRING,
  global_attribute14 STRING,
  global_attribute15 STRING,
  global_attribute16 STRING,
  global_attribute17 STRING,
  global_attribute18 STRING,
  global_attribute19 STRING,
  global_attribute20 STRING,
  external_bank_account_id DECIMAL(15, 0),
  accounting_event_id DECIMAL(15, 0),
  mrc_exchange_date STRING,
  mrc_exchange_rate STRING,
  mrc_exchange_rate_type STRING,
  mrc_gain_code_combination_id STRING,
  mrc_invoice_base_amount STRING,
  mrc_loss_code_combination_id STRING,
  mrc_payment_base_amount STRING,
  reversal_flag STRING,
  reversal_inv_pmt_id DECIMAL(15, 0),
  iban_number STRING,
  invoicing_party_id DECIMAL(15, 0),
  invoicing_party_site_id DECIMAL(15, 0),
  invoicing_vendor_site_id DECIMAL(15, 0),
  remit_to_supplier_name STRING,
  remit_to_supplier_id DECIMAL(15, 0),
  remit_to_supplier_site STRING,
  remit_to_supplier_site_id DECIMAL(15, 0),
  accounting_date TIMESTAMP,
  accrual_posted_flag STRING,
  amount DECIMAL(28, 10),
  cash_posted_flag STRING,
  check_id DECIMAL(15, 0),
  invoice_id DECIMAL(15, 0),
  invoice_payment_id DECIMAL(15, 0),
  last_updated_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  payment_num DECIMAL(15, 0),
  period_name STRING,
  posted_flag STRING,
  set_of_books_id DECIMAL(15, 0),
  accts_pay_code_combination_id DECIMAL(15, 0),
  asset_code_combination_id DECIMAL(15, 0),
  created_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  last_update_login DECIMAL(15, 0),
  bank_account_num STRING,
  bank_account_type STRING,
  bank_num STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.ap_invoice_payments_all (
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
  IS_DELETED_FLG,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_invoice_payments_all
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_invoice_payments_all';