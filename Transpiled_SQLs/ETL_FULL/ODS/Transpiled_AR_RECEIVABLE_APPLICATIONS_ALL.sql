DROP TABLE IF EXISTS silver_bec_ods.AR_RECEIVABLE_APPLICATIONS_ALL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.ar_receivable_applications_all (
  receivable_application_id DECIMAL(15, 0),
  last_updated_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  amount_applied DECIMAL(28, 10),
  gl_date TIMESTAMP,
  code_combination_id DECIMAL(15, 0),
  set_of_books_id DECIMAL(15, 0),
  display STRING,
  apply_date TIMESTAMP,
  application_type STRING,
  status STRING,
  payment_schedule_id DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  cash_receipt_id DECIMAL(15, 0),
  applied_customer_trx_id DECIMAL(15, 0),
  applied_customer_trx_line_id DECIMAL(15, 0),
  applied_payment_schedule_id DECIMAL(15, 0),
  customer_trx_id DECIMAL(15, 0),
  line_applied DECIMAL(28, 10),
  tax_applied DECIMAL(28, 10),
  freight_applied DECIMAL(28, 10),
  receivables_charges_applied DECIMAL(28, 10),
  on_account_customer DECIMAL(15, 0),
  receivables_trx_id DECIMAL(15, 0),
  earned_discount_taken DECIMAL(28, 10),
  unearned_discount_taken DECIMAL(28, 10),
  days_late DECIMAL(15, 0),
  application_rule STRING,
  gl_posted_date TIMESTAMP,
  comments STRING,
  attribute_category STRING,
  attribute1 STRING,
  attribute2 STRING,
  attribute3 STRING,
  attribute4 STRING,
  attribute5 STRING,
  attribute6 STRING,
  attribute7 STRING,
  attribute8 STRING,
  attribute9 STRING,
  attribute10 STRING,
  postable STRING,
  posting_control_id DECIMAL(15, 0),
  acctd_amount_applied_from DECIMAL(28, 10),
  acctd_amount_applied_to DECIMAL(28, 10),
  acctd_earned_discount_taken DECIMAL(28, 10),
  attribute11 STRING,
  attribute12 STRING,
  attribute13 STRING,
  attribute14 STRING,
  attribute15 STRING,
  confirmed_flag STRING,
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  request_id DECIMAL(15, 0),
  ussgl_transaction_code STRING,
  ussgl_transaction_code_context STRING,
  earned_discount_ccid DECIMAL(15, 0),
  unearned_discount_ccid DECIMAL(15, 0),
  acctd_unearned_discount_taken DECIMAL(28, 10),
  reversal_gl_date TIMESTAMP,
  cash_receipt_history_id DECIMAL(15, 0),
  org_id DECIMAL(15, 0),
  tax_code STRING,
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
  global_attribute_category STRING,
  cons_inv_id DECIMAL(15, 0),
  cons_inv_id_to DECIMAL(15, 0),
  amount_applied_from DECIMAL(28, 10),
  trans_to_receipt_rate DECIMAL(28, 10),
  rule_set_id DECIMAL(15, 0),
  line_ediscounted DECIMAL(28, 10),
  tax_ediscounted DECIMAL(28, 10),
  freight_ediscounted DECIMAL(28, 10),
  charges_ediscounted DECIMAL(28, 10),
  line_uediscounted DECIMAL(28, 10),
  tax_uediscounted DECIMAL(28, 10),
  freight_uediscounted DECIMAL(28, 10),
  charges_uediscounted DECIMAL(28, 10),
  mrc_amount_applied STRING,
  mrc_amount_applied_from STRING,
  mrc_display STRING,
  mrc_status STRING,
  mrc_payment_schedule_id STRING,
  mrc_cash_receipt_id STRING,
  mrc_gl_posted_date STRING,
  mrc_posting_control_id STRING,
  mrc_acctd_amount_applied_from STRING,
  mrc_acctd_amount_applied_to STRING,
  mrc_acctd_earned_disc_taken STRING,
  mrc_acctd_unearned_disc_taken STRING,
  edisc_tax_acct_rule STRING,
  unedisc_tax_acct_rule STRING,
  link_to_trx_hist_id DECIMAL(15, 0),
  link_to_customer_trx_id DECIMAL(15, 0),
  application_ref_type STRING,
  application_ref_id DECIMAL(15, 0),
  application_ref_num STRING,
  chargeback_customer_trx_id DECIMAL(15, 0),
  secondary_application_ref_id DECIMAL(15, 0),
  payment_set_id DECIMAL(15, 0),
  application_ref_reason STRING,
  customer_reference STRING,
  customer_reason STRING,
  applied_rec_app_id DECIMAL(15, 0),
  secondary_application_ref_type STRING,
  secondary_application_ref_num STRING,
  event_id DECIMAL(15, 0),
  upgrade_method STRING,
  ax_accounted_flag STRING,
  INCLUDE_IN_ACCUMULATION STRING,
  ON_ACCT_CUST_ID DECIMAL(15, 0),
  ON_ACCT_CUST_SITE_USE_ID DECIMAL(15, 0),
  ON_ACCT_PO_NUM STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.AR_RECEIVABLE_APPLICATIONS_ALL (
  receivable_application_id,
  last_updated_by,
  last_update_date,
  created_by,
  creation_date,
  amount_applied,
  gl_date,
  code_combination_id,
  set_of_books_id,
  display,
  apply_date,
  application_type,
  status,
  payment_schedule_id,
  last_update_login,
  cash_receipt_id,
  applied_customer_trx_id,
  applied_customer_trx_line_id,
  applied_payment_schedule_id,
  customer_trx_id,
  line_applied,
  tax_applied,
  freight_applied,
  receivables_charges_applied,
  on_account_customer,
  receivables_trx_id,
  earned_discount_taken,
  unearned_discount_taken,
  days_late,
  application_rule,
  gl_posted_date,
  comments,
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
  postable,
  posting_control_id,
  acctd_amount_applied_from,
  acctd_amount_applied_to,
  acctd_earned_discount_taken,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  confirmed_flag,
  program_application_id,
  program_id,
  program_update_date,
  request_id,
  ussgl_transaction_code,
  ussgl_transaction_code_context,
  earned_discount_ccid,
  unearned_discount_ccid,
  acctd_unearned_discount_taken,
  reversal_gl_date,
  cash_receipt_history_id,
  org_id,
  tax_code,
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
  global_attribute_category,
  cons_inv_id,
  cons_inv_id_to,
  amount_applied_from,
  trans_to_receipt_rate,
  rule_set_id,
  line_ediscounted,
  tax_ediscounted,
  freight_ediscounted,
  charges_ediscounted,
  line_uediscounted,
  tax_uediscounted,
  freight_uediscounted,
  charges_uediscounted,
  mrc_amount_applied,
  mrc_amount_applied_from,
  mrc_display,
  mrc_status,
  mrc_payment_schedule_id,
  mrc_cash_receipt_id,
  mrc_gl_posted_date,
  mrc_posting_control_id,
  mrc_acctd_amount_applied_from,
  mrc_acctd_amount_applied_to,
  mrc_acctd_earned_disc_taken,
  mrc_acctd_unearned_disc_taken,
  edisc_tax_acct_rule,
  unedisc_tax_acct_rule,
  link_to_trx_hist_id,
  link_to_customer_trx_id,
  application_ref_type,
  application_ref_id,
  application_ref_num,
  chargeback_customer_trx_id,
  secondary_application_ref_id,
  payment_set_id,
  application_ref_reason,
  customer_reference,
  customer_reason,
  applied_rec_app_id,
  secondary_application_ref_type,
  secondary_application_ref_num,
  event_id,
  upgrade_method,
  ax_accounted_flag,
  include_in_accumulation,
  on_acct_cust_id,
  on_acct_cust_site_use_id,
  on_acct_po_num,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  receivable_application_id,
  last_updated_by,
  last_update_date,
  created_by,
  creation_date,
  amount_applied,
  gl_date,
  code_combination_id,
  set_of_books_id,
  display,
  apply_date,
  application_type,
  status,
  payment_schedule_id,
  last_update_login,
  cash_receipt_id,
  applied_customer_trx_id,
  applied_customer_trx_line_id,
  applied_payment_schedule_id,
  customer_trx_id,
  line_applied,
  tax_applied,
  freight_applied,
  receivables_charges_applied,
  on_account_customer,
  receivables_trx_id,
  earned_discount_taken,
  unearned_discount_taken,
  days_late,
  application_rule,
  gl_posted_date,
  comments,
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
  postable,
  posting_control_id,
  acctd_amount_applied_from,
  acctd_amount_applied_to,
  acctd_earned_discount_taken,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  confirmed_flag,
  program_application_id,
  program_id,
  program_update_date,
  request_id,
  ussgl_transaction_code,
  ussgl_transaction_code_context,
  earned_discount_ccid,
  unearned_discount_ccid,
  acctd_unearned_discount_taken,
  reversal_gl_date,
  cash_receipt_history_id,
  org_id,
  tax_code,
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
  global_attribute_category,
  cons_inv_id,
  cons_inv_id_to,
  amount_applied_from,
  trans_to_receipt_rate,
  rule_set_id,
  line_ediscounted,
  tax_ediscounted,
  freight_ediscounted,
  charges_ediscounted,
  line_uediscounted,
  tax_uediscounted,
  freight_uediscounted,
  charges_uediscounted,
  mrc_amount_applied,
  mrc_amount_applied_from,
  mrc_display,
  mrc_status,
  mrc_payment_schedule_id,
  mrc_cash_receipt_id,
  mrc_gl_posted_date,
  mrc_posting_control_id,
  mrc_acctd_amount_applied_from,
  mrc_acctd_amount_applied_to,
  mrc_acctd_earned_disc_taken,
  mrc_acctd_unearned_disc_taken,
  edisc_tax_acct_rule,
  unedisc_tax_acct_rule,
  link_to_trx_hist_id,
  link_to_customer_trx_id,
  application_ref_type,
  application_ref_id,
  application_ref_num,
  chargeback_customer_trx_id,
  secondary_application_ref_id,
  payment_set_id,
  application_ref_reason,
  customer_reference,
  customer_reason,
  applied_rec_app_id,
  secondary_application_ref_type,
  secondary_application_ref_num,
  event_id,
  upgrade_method,
  ax_accounted_flag,
  include_in_accumulation,
  on_acct_cust_id,
  on_acct_cust_site_use_id,
  on_acct_po_num,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.AR_RECEIVABLE_APPLICATIONS_ALL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ar_receivable_applications_all';