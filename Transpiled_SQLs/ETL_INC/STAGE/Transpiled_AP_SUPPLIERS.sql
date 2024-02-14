TRUNCATE table bronze_bec_ods_stg.AP_SUPPLIERS;
INSERT INTO bronze_bec_ods_stg.AP_SUPPLIERS (
  hold_future_payments_flag,
  hold_reason,
  distribution_set_id,
  accts_pay_code_combination_id,
  disc_lost_code_combination_id,
  disc_taken_code_combination_id,
  expense_code_combination_id,
  prepay_code_combination_id,
  num_1099,
  type_1099,
  withholding_status_lookup_code,
  withholding_start_date,
  organization_type_lookup_code,
  vat_code,
  start_date_active,
  end_date_active,
  minority_group_lookup_code,
  payment_method_lookup_code,
  bank_account_name,
  bank_account_num,
  bank_num,
  bank_account_type,
  women_owned_flag,
  small_business_flag,
  standard_industry_class,
  hold_flag,
  purchasing_hold_reason,
  hold_by,
  hold_date,
  terms_date_basis,
  price_tolerance,
  inspection_required_flag,
  receipt_required_flag,
  qty_rcv_tolerance,
  qty_rcv_exception_code,
  enforce_ship_to_location_code,
  days_early_receipt_allowed,
  days_late_receipt_allowed,
  receipt_days_exception_code,
  receiving_routing_id,
  allow_substitute_receipts_flag,
  allow_unordered_receipts_flag,
  hold_unmatched_invoices_flag,
  exclusive_payment_flag,
  ap_tax_rounding_rule,
  auto_tax_calc_flag,
  auto_tax_calc_override,
  amount_includes_tax_flag,
  tax_verification_date,
  name_control,
  state_reportable_flag,
  federal_reportable_flag,
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
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  offset_vat_code,
  vat_registration_num,
  auto_calculate_interest_flag,
  validation_number,
  exclude_freight_from_discount,
  tax_reporting_name,
  check_digits,
  bank_number,
  allow_awt_flag,
  awt_group_id,
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
  vendor_id,
  last_update_date,
  last_updated_by,
  vendor_name,
  vendor_name_alt,
  segment1,
  summary_flag,
  enabled_flag,
  segment2,
  segment3,
  segment4,
  segment5,
  last_update_login,
  creation_date,
  created_by,
  employee_id,
  vendor_type_lookup_code,
  customer_num,
  one_time_flag,
  parent_vendor_id,
  min_order_amount,
  ship_to_location_id,
  bill_to_location_id,
  ship_via_lookup_code,
  freight_terms_lookup_code,
  fob_lookup_code,
  terms_id,
  set_of_books_id,
  credit_status_lookup_code,
  credit_limit,
  always_take_disc_flag,
  pay_date_basis_lookup_code,
  pay_group_lookup_code,
  payment_priority,
  invoice_currency_code,
  payment_currency_code,
  invoice_amount_limit,
  exchange_date_lookup_code,
  hold_all_payments_flag,
  global_attribute_category,
  edi_transaction_handling,
  edi_payment_method,
  edi_payment_format,
  edi_remittance_method,
  edi_remittance_instruction,
  bank_charge_bearer,
  bank_branch_type,
  match_option,
  future_dated_payment_ccid,
  create_debit_memo_flag,
  offset_tax_flag,
  party_id,
  parent_party_id,
  ni_number,
  tca_sync_num_1099,
  tca_sync_vendor_name,
  tca_sync_vat_reg_num,
  individual_1099,
  unique_tax_reference_num,
  partnership_utr,
  partnership_name,
  cis_enabled_flag,
  first_name,
  second_name,
  last_name,
  salutation,
  trading_name,
  work_reference,
  company_registration_number,
  national_insurance_number,
  verification_number,
  verification_request_id,
  match_status_flag,
  cis_verification_date,
  cis_parent_vendor_id,
  pay_awt_group_id,
  bus_class_last_certified_date,
  bus_class_last_certified_by,
  `VAT_REGISTRATION_NUM#1`,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    hold_future_payments_flag,
    hold_reason,
    distribution_set_id,
    accts_pay_code_combination_id,
    disc_lost_code_combination_id,
    disc_taken_code_combination_id,
    expense_code_combination_id,
    prepay_code_combination_id,
    num_1099,
    type_1099,
    withholding_status_lookup_code,
    withholding_start_date,
    organization_type_lookup_code,
    vat_code,
    start_date_active,
    end_date_active,
    minority_group_lookup_code,
    payment_method_lookup_code,
    bank_account_name,
    bank_account_num,
    bank_num,
    bank_account_type,
    women_owned_flag,
    small_business_flag,
    standard_industry_class,
    hold_flag,
    purchasing_hold_reason,
    hold_by,
    hold_date,
    terms_date_basis,
    price_tolerance,
    inspection_required_flag,
    receipt_required_flag,
    qty_rcv_tolerance,
    qty_rcv_exception_code,
    enforce_ship_to_location_code,
    days_early_receipt_allowed,
    days_late_receipt_allowed,
    receipt_days_exception_code,
    receiving_routing_id,
    allow_substitute_receipts_flag,
    allow_unordered_receipts_flag,
    hold_unmatched_invoices_flag,
    exclusive_payment_flag,
    ap_tax_rounding_rule,
    auto_tax_calc_flag,
    auto_tax_calc_override,
    amount_includes_tax_flag,
    tax_verification_date,
    name_control,
    state_reportable_flag,
    federal_reportable_flag,
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
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    offset_vat_code,
    vat_registration_num,
    auto_calculate_interest_flag,
    validation_number,
    exclude_freight_from_discount,
    tax_reporting_name,
    check_digits,
    bank_number,
    allow_awt_flag,
    awt_group_id,
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
    vendor_id,
    last_update_date,
    last_updated_by,
    vendor_name,
    vendor_name_alt,
    segment1,
    summary_flag,
    enabled_flag,
    segment2,
    segment3,
    segment4,
    segment5,
    last_update_login,
    creation_date,
    created_by,
    employee_id,
    vendor_type_lookup_code,
    customer_num,
    one_time_flag,
    parent_vendor_id,
    min_order_amount,
    ship_to_location_id,
    bill_to_location_id,
    ship_via_lookup_code,
    freight_terms_lookup_code,
    fob_lookup_code,
    terms_id,
    set_of_books_id,
    credit_status_lookup_code,
    credit_limit,
    always_take_disc_flag,
    pay_date_basis_lookup_code,
    pay_group_lookup_code,
    payment_priority,
    invoice_currency_code,
    payment_currency_code,
    invoice_amount_limit,
    exchange_date_lookup_code,
    hold_all_payments_flag,
    global_attribute_category,
    edi_transaction_handling,
    edi_payment_method,
    edi_payment_format,
    edi_remittance_method,
    edi_remittance_instruction,
    bank_charge_bearer,
    bank_branch_type,
    match_option,
    future_dated_payment_ccid,
    create_debit_memo_flag,
    offset_tax_flag,
    party_id,
    parent_party_id,
    ni_number,
    tca_sync_num_1099,
    tca_sync_vendor_name,
    tca_sync_vat_reg_num,
    individual_1099,
    unique_tax_reference_num,
    partnership_utr,
    partnership_name,
    cis_enabled_flag,
    first_name,
    second_name,
    last_name,
    salutation,
    trading_name,
    work_reference,
    company_registration_number,
    national_insurance_number,
    verification_number,
    verification_request_id,
    match_status_flag,
    cis_verification_date,
    cis_parent_vendor_id,
    pay_awt_group_id,
    bus_class_last_certified_date,
    bus_class_last_certified_by,
    `VAT_REGISTRATION_NUM#1`,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AP_SUPPLIERS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (vendor_id, KCA_SEQ_ID) IN (
      SELECT
        vendor_id,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.AP_SUPPLIERS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        vendor_id
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_suppliers'
    )
);