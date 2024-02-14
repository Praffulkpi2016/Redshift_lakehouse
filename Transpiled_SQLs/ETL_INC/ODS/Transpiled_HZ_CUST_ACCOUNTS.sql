/* Delete Records */
DELETE FROM silver_bec_ods.HZ_CUST_ACCOUNTS
WHERE
  cust_account_id IN (
    SELECT
      stg.cust_account_id
    FROM silver_bec_ods.HZ_CUST_ACCOUNTS AS ods, bronze_bec_ods_stg.HZ_CUST_ACCOUNTS AS stg
    WHERE
      ods.cust_account_id = stg.cust_account_id
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.HZ_CUST_ACCOUNTS (
  cust_account_id,
  party_id,
  last_update_date,
  account_number,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  wh_update_date,
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
  attribute16,
  attribute17,
  attribute18,
  attribute19,
  attribute20,
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
  orig_system_reference,
  status,
  customer_type,
  customer_class_code,
  primary_salesrep_id,
  sales_channel_code,
  order_type_id,
  price_list_id,
  subcategory_code,
  tax_code,
  fob_point,
  freight_term,
  ship_partial,
  ship_via,
  warehouse_id,
  payment_term_id,
  tax_header_level_flag,
  tax_rounding_rule,
  coterminate_day_month,
  primary_specialist_id,
  secondary_specialist_id,
  account_liable_flag,
  restriction_limit_amount,
  current_balance,
  password_text,
  high_priority_indicator,
  account_established_date,
  account_termination_date,
  account_activation_date,
  credit_classification_code,
  department,
  major_account_number,
  hotwatch_service_flag,
  hotwatch_svc_bal_ind,
  held_bill_expiration_date,
  hold_bill_flag,
  high_priority_remarks,
  po_effective_date,
  po_expiration_date,
  realtime_rate_flag,
  single_user_flag,
  watch_account_flag,
  watch_balance_indicator,
  geo_code,
  acct_life_cycle_status,
  account_name,
  deposit_refund_method,
  dormant_account_flag,
  npa_number,
  pin_number,
  suspension_date,
  write_off_adjustment_amount,
  write_off_payment_amount,
  write_off_amount,
  source_code,
  competitor_type,
  comments,
  dates_negative_tolerance,
  dates_positive_tolerance,
  date_type_preference,
  over_shipment_tolerance,
  under_shipment_tolerance,
  over_return_tolerance,
  under_return_tolerance,
  item_cross_ref_pref,
  ship_sets_include_lines_flag,
  arrivalsets_include_lines_flag,
  sched_date_push_flag,
  invoice_quantity_rule,
  pricing_event,
  account_replication_key,
  status_update_date,
  autopay_flag,
  notify_flag,
  last_batch_id,
  org_id,
  object_version_number,
  created_by_module,
  application_id,
  selling_party_id,
  ADVANCE_PAYMENT_INDICATOR,
  DUNS_EXTENSION,
  FEDERAL_ENTITY_TYPE,
  TRADING_PARTNER_AGENCY_ID,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    cust_account_id,
    party_id,
    last_update_date,
    account_number,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    wh_update_date,
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
    attribute16,
    attribute17,
    attribute18,
    attribute19,
    attribute20,
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
    orig_system_reference,
    status,
    customer_type,
    customer_class_code,
    primary_salesrep_id,
    sales_channel_code,
    order_type_id,
    price_list_id,
    subcategory_code,
    tax_code,
    fob_point,
    freight_term,
    ship_partial,
    ship_via,
    warehouse_id,
    payment_term_id,
    tax_header_level_flag,
    tax_rounding_rule,
    coterminate_day_month,
    primary_specialist_id,
    secondary_specialist_id,
    account_liable_flag,
    restriction_limit_amount,
    current_balance,
    password_text,
    high_priority_indicator,
    account_established_date,
    account_termination_date,
    account_activation_date,
    credit_classification_code,
    department,
    major_account_number,
    hotwatch_service_flag,
    hotwatch_svc_bal_ind,
    held_bill_expiration_date,
    hold_bill_flag,
    high_priority_remarks,
    po_effective_date,
    po_expiration_date,
    realtime_rate_flag,
    single_user_flag,
    watch_account_flag,
    watch_balance_indicator,
    geo_code,
    acct_life_cycle_status,
    account_name,
    deposit_refund_method,
    dormant_account_flag,
    npa_number,
    pin_number,
    suspension_date,
    write_off_adjustment_amount,
    write_off_payment_amount,
    write_off_amount,
    source_code,
    competitor_type,
    comments,
    dates_negative_tolerance,
    dates_positive_tolerance,
    date_type_preference,
    over_shipment_tolerance,
    under_shipment_tolerance,
    over_return_tolerance,
    under_return_tolerance,
    item_cross_ref_pref,
    ship_sets_include_lines_flag,
    arrivalsets_include_lines_flag,
    sched_date_push_flag,
    invoice_quantity_rule,
    pricing_event,
    account_replication_key,
    status_update_date,
    autopay_flag,
    notify_flag,
    last_batch_id,
    org_id,
    object_version_number,
    created_by_module,
    application_id,
    selling_party_id,
    ADVANCE_PAYMENT_INDICATOR,
    DUNS_EXTENSION,
    FEDERAL_ENTITY_TYPE,
    TRADING_PARTNER_AGENCY_ID,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.HZ_CUST_ACCOUNTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (cust_account_id, kca_seq_id) IN (
      SELECT
        cust_account_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.HZ_CUST_ACCOUNTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        cust_account_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.HZ_CUST_ACCOUNTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.HZ_CUST_ACCOUNTS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    cust_account_id
  ) IN (
    SELECT
      cust_account_id
    FROM bec_raw_dl_ext.HZ_CUST_ACCOUNTS
    WHERE
      (cust_account_id, KCA_SEQ_ID) IN (
        SELECT
          cust_account_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.HZ_CUST_ACCOUNTS
        GROUP BY
          cust_account_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'hz_cust_accounts';