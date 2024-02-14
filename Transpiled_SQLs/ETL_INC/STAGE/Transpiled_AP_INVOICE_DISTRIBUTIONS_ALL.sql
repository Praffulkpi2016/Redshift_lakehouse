TRUNCATE table bronze_bec_ods_stg.AP_INVOICE_DISTRIBUTIONS_ALL;
INSERT INTO bronze_bec_ods_stg.AP_INVOICE_DISTRIBUTIONS_ALL (
  accounting_date,
  accounting_event_id,
  accrual_posted_flag,
  accts_pay_code_combination_id,
  adjustment_reason,
  amount,
  amount_at_prepay_pay_xrate,
  amount_at_prepay_xrate,
  amount_encumbered,
  amount_includes_tax_flag,
  amount_to_post,
  amount_variance,
  asset_book_type_code,
  asset_category_id,
  assets_addition_flag,
  assets_tracking_flag,
  attribute_category,
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
  award_id,
  awt_flag,
  awt_gross_amount,
  awt_group_id,
  awt_invoice_id,
  awt_invoice_payment_id,
  awt_origin_group_id,
  awt_related_id,
  awt_tax_rate_id,
  awt_withheld_amt,
  base_amount,
  base_amount_encumbered,
  base_amount_to_post,
  base_amount_variance,
  base_invoice_price_variance,
  base_quantity_variance,
  batch_id,
  bc_event_id,
  cancellation_flag,
  cancelled_flag,
  cash_basis_final_app_rounding,
  cash_je_batch_id,
  cash_posted_flag,
  cc_reversal_flag,
  charge_applicable_to_dist_id,
  company_prepaid_invoice_id,
  corrected_invoice_dist_id,
  corrected_quantity,
  country_of_supply,
  created_by,
  creation_date,
  credit_card_trx_id,
  daily_amount,
  description,
  detail_tax_dist_id,
  dist_code_combination_id,
  dist_match_type,
  distribution_class,
  distribution_line_number,
  earliest_settlement_date,
  encumbered_flag,
  end_expense_date,
  exchange_date,
  exchange_rate,
  exchange_rate_type,
  exchange_rate_variance,
  expenditure_item_date,
  expenditure_organization_id,
  expenditure_type,
  expense_group,
  extra_po_erv,
  final_application_rounding,
  final_match_flag,
  final_payment_rounding,
  final_release_rounding,
  fully_paid_acctd_flag,
  global_attribute_category,
  global_attribute1,
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
  global_attribute2,
  global_attribute20,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  gms_burdenable_raw_cost,
  historical_flag,
  income_tax_region,
  intended_use,
  inventory_transfer_status,
  invoice_distribution_id,
  invoice_id,
  invoice_includes_prepay_flag,
  invoice_line_number,
  invoice_price_variance,
  je_batch_id,
  justification,
  last_update_date,
  last_update_login,
  last_updated_by,
  line_group_number,
  line_type_lookup_code,
  match_status_flag,
  matched_uom_lookup_code,
  merchant_document_number,
  merchant_name,
  merchant_reference,
  merchant_tax_reg_number,
  merchant_taxpayer_id,
  mrc_accrual_posted_flag,
  mrc_amount,
  mrc_amount_to_post,
  mrc_base_amount,
  mrc_base_amount_to_post,
  mrc_base_inv_price_variance,
  mrc_cash_je_batch_id,
  mrc_cash_posted_flag,
  mrc_dist_code_combination_id,
  mrc_exchange_date,
  mrc_exchange_rate,
  mrc_exchange_rate_type,
  mrc_exchange_rate_variance,
  mrc_je_batch_id,
  mrc_posted_amount,
  mrc_posted_base_amount,
  mrc_posted_flag,
  mrc_program_application_id,
  mrc_program_id,
  mrc_program_update_date,
  mrc_rate_var_ccid,
  mrc_receipt_conversion_rate,
  mrc_request_id,
  old_dist_line_number,
  old_distribution_id,
  org_id,
  other_invoice_id,
  pa_addition_flag,
  pa_cc_ar_invoice_id,
  pa_cc_ar_invoice_line_num,
  pa_cc_processed_code,
  pa_cmt_xface_flag,
  pa_quantity,
  packet_id,
  parent_invoice_id,
  parent_reversal_id,
  pay_awt_group_id,
  period_name,
  po_distribution_id,
  posted_amount,
  posted_base_amount,
  posted_flag,
  prepay_amount_remaining,
  prepay_distribution_id,
  prepay_tax_diff_amount,
  prepay_tax_parent_id,
  price_adjustment_flag,
  price_correct_inv_id,
  price_correct_qty,
  price_var_code_combination_id,
  program_application_id,
  program_id,
  program_update_date,
  project_accounting_context,
  project_id,
  quantity_invoiced,
  quantity_unencumbered,
  quantity_variance,
  rate_var_code_combination_id,
  rcv_charge_addition_flag,
  rcv_transaction_id,
  rec_nrec_rate,
  receipt_conversion_rate,
  receipt_currency_amount,
  receipt_currency_code,
  receipt_missing_flag,
  receipt_required_flag,
  receipt_verified_flag,
  recovery_rate_code,
  recovery_rate_id,
  recovery_rate_name,
  recovery_type_code,
  recurring_payment_id,
  reference_1,
  reference_2,
  related_id,
  related_retainage_dist_id,
  release_inv_dist_derived_from,
  req_distribution_id,
  request_id,
  retained_amount_remaining,
  retained_invoice_dist_id,
  reversal_flag,
  root_distribution_id,
  rounding_amt,
  set_of_books_id,
  start_expense_date,
  stat_amount,
  summary_tax_line_id,
  task_id,
  tax_already_distributed_flag,
  tax_calculated_flag,
  tax_code_id,
  tax_code_override_flag,
  tax_recoverable_flag,
  tax_recovery_override_flag,
  tax_recovery_rate,
  taxable_amount,
  taxable_base_amount,
  total_dist_amount,
  total_dist_base_amount,
  type_1099,
  unit_price,
  upgrade_base_posted_amt,
  upgrade_posted_amt,
  ussgl_transaction_code,
  ussgl_trx_code_context,
  vat_code,
  web_parameter_id,
  withholding_tax_code_id,
  xinv_parent_reversal_id,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    accounting_date,
    accounting_event_id,
    accrual_posted_flag,
    accts_pay_code_combination_id,
    adjustment_reason,
    amount,
    amount_at_prepay_pay_xrate,
    amount_at_prepay_xrate,
    amount_encumbered,
    amount_includes_tax_flag,
    amount_to_post,
    amount_variance,
    asset_book_type_code,
    asset_category_id,
    assets_addition_flag,
    assets_tracking_flag,
    attribute_category,
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
    award_id,
    awt_flag,
    awt_gross_amount,
    awt_group_id,
    awt_invoice_id,
    awt_invoice_payment_id,
    awt_origin_group_id,
    awt_related_id,
    awt_tax_rate_id,
    awt_withheld_amt,
    base_amount,
    base_amount_encumbered,
    base_amount_to_post,
    base_amount_variance,
    base_invoice_price_variance,
    base_quantity_variance,
    batch_id,
    bc_event_id,
    cancellation_flag,
    cancelled_flag,
    cash_basis_final_app_rounding,
    cash_je_batch_id,
    cash_posted_flag,
    cc_reversal_flag,
    charge_applicable_to_dist_id,
    company_prepaid_invoice_id,
    corrected_invoice_dist_id,
    corrected_quantity,
    country_of_supply,
    created_by,
    creation_date,
    credit_card_trx_id,
    daily_amount,
    description,
    detail_tax_dist_id,
    dist_code_combination_id,
    dist_match_type,
    distribution_class,
    distribution_line_number,
    earliest_settlement_date,
    encumbered_flag,
    end_expense_date,
    exchange_date,
    exchange_rate,
    exchange_rate_type,
    exchange_rate_variance,
    expenditure_item_date,
    expenditure_organization_id,
    expenditure_type,
    expense_group,
    extra_po_erv,
    final_application_rounding,
    final_match_flag,
    final_payment_rounding,
    final_release_rounding,
    fully_paid_acctd_flag,
    global_attribute_category,
    global_attribute1,
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
    global_attribute2,
    global_attribute20,
    global_attribute3,
    global_attribute4,
    global_attribute5,
    global_attribute6,
    global_attribute7,
    global_attribute8,
    global_attribute9,
    gms_burdenable_raw_cost,
    historical_flag,
    income_tax_region,
    intended_use,
    inventory_transfer_status,
    invoice_distribution_id,
    invoice_id,
    invoice_includes_prepay_flag,
    invoice_line_number,
    invoice_price_variance,
    je_batch_id,
    justification,
    last_update_date,
    last_update_login,
    last_updated_by,
    line_group_number,
    line_type_lookup_code,
    match_status_flag,
    matched_uom_lookup_code,
    merchant_document_number,
    merchant_name,
    merchant_reference,
    merchant_tax_reg_number,
    merchant_taxpayer_id,
    mrc_accrual_posted_flag,
    mrc_amount,
    mrc_amount_to_post,
    mrc_base_amount,
    mrc_base_amount_to_post,
    mrc_base_inv_price_variance,
    mrc_cash_je_batch_id,
    mrc_cash_posted_flag,
    mrc_dist_code_combination_id,
    mrc_exchange_date,
    mrc_exchange_rate,
    mrc_exchange_rate_type,
    mrc_exchange_rate_variance,
    mrc_je_batch_id,
    mrc_posted_amount,
    mrc_posted_base_amount,
    mrc_posted_flag,
    mrc_program_application_id,
    mrc_program_id,
    mrc_program_update_date,
    mrc_rate_var_ccid,
    mrc_receipt_conversion_rate,
    mrc_request_id,
    old_dist_line_number,
    old_distribution_id,
    org_id,
    other_invoice_id,
    pa_addition_flag,
    pa_cc_ar_invoice_id,
    pa_cc_ar_invoice_line_num,
    pa_cc_processed_code,
    pa_cmt_xface_flag,
    pa_quantity,
    packet_id,
    parent_invoice_id,
    parent_reversal_id,
    pay_awt_group_id,
    period_name,
    po_distribution_id,
    posted_amount,
    posted_base_amount,
    posted_flag,
    prepay_amount_remaining,
    prepay_distribution_id,
    prepay_tax_diff_amount,
    prepay_tax_parent_id,
    price_adjustment_flag,
    price_correct_inv_id,
    price_correct_qty,
    price_var_code_combination_id,
    program_application_id,
    program_id,
    program_update_date,
    project_accounting_context,
    project_id,
    quantity_invoiced,
    quantity_unencumbered,
    quantity_variance,
    rate_var_code_combination_id,
    rcv_charge_addition_flag,
    rcv_transaction_id,
    rec_nrec_rate,
    receipt_conversion_rate,
    receipt_currency_amount,
    receipt_currency_code,
    receipt_missing_flag,
    receipt_required_flag,
    receipt_verified_flag,
    recovery_rate_code,
    recovery_rate_id,
    recovery_rate_name,
    recovery_type_code,
    recurring_payment_id,
    reference_1,
    reference_2,
    related_id,
    related_retainage_dist_id,
    release_inv_dist_derived_from,
    req_distribution_id,
    request_id,
    retained_amount_remaining,
    retained_invoice_dist_id,
    reversal_flag,
    root_distribution_id,
    rounding_amt,
    set_of_books_id,
    start_expense_date,
    stat_amount,
    summary_tax_line_id,
    task_id,
    tax_already_distributed_flag,
    tax_calculated_flag,
    tax_code_id,
    tax_code_override_flag,
    tax_recoverable_flag,
    tax_recovery_override_flag,
    tax_recovery_rate,
    taxable_amount,
    taxable_base_amount,
    total_dist_amount,
    total_dist_base_amount,
    type_1099,
    unit_price,
    upgrade_base_posted_amt,
    upgrade_posted_amt,
    ussgl_transaction_code,
    ussgl_trx_code_context,
    vat_code,
    web_parameter_id,
    withholding_tax_code_id,
    xinv_parent_reversal_id,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AP_INVOICE_DISTRIBUTIONS_ALL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (invoice_distribution_id, KCA_SEQ_ID) IN (
      SELECT
        invoice_distribution_id,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.AP_INVOICE_DISTRIBUTIONS_ALL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        invoice_distribution_id
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_invoice_distributions_all'
    )
);