/* Delete Records */
DELETE FROM silver_bec_ods.AP_PAYMENT_SCHEDULES_ALL
WHERE
  (INVOICE_ID, PAYMENT_NUM) IN (
    SELECT
      stg.INVOICE_ID,
      stg.PAYMENT_NUM
    FROM silver_bec_ods.AP_PAYMENT_SCHEDULES_ALL AS ods, bronze_bec_ods_stg.AP_PAYMENT_SCHEDULES_ALL AS stg
    WHERE
      ods.INVOICE_ID = stg.INVOICE_ID
      AND ods.PAYMENT_NUM = stg.PAYMENT_NUM
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.AP_PAYMENT_SCHEDULES_ALL (
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
  inv_curr_gross_amount,
  checkrun_id,
  dbi_events_complete_flag,
  iby_hold_reason,
  payment_method_code,
  remittance_message1,
  remittance_message2,
  remittance_message3,
  remit_to_supplier_name,
  remit_to_supplier_id,
  remit_to_supplier_site,
  remit_to_supplier_site_id,
  relationship_id,
  invoice_id,
  last_updated_by,
  last_update_date,
  payment_cross_rate,
  payment_num,
  amount_remaining,
  created_by,
  creation_date,
  discount_date,
  due_date,
  future_pay_due_date,
  gross_amount,
  hold_flag,
  last_update_login,
  payment_method_lookup_code,
  payment_priority,
  payment_status_flag,
  second_discount_date,
  third_discount_date,
  batch_id,
  discount_amount_available,
  second_disc_amt_available,
  third_disc_amt_available,
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
  discount_amount_remaining,
  org_id,
  global_attribute_category,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
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
    inv_curr_gross_amount,
    checkrun_id,
    dbi_events_complete_flag,
    iby_hold_reason,
    payment_method_code,
    remittance_message1,
    remittance_message2,
    remittance_message3,
    remit_to_supplier_name,
    remit_to_supplier_id,
    remit_to_supplier_site,
    remit_to_supplier_site_id,
    relationship_id,
    invoice_id,
    last_updated_by,
    last_update_date,
    payment_cross_rate,
    payment_num,
    amount_remaining,
    created_by,
    creation_date,
    discount_date,
    due_date,
    future_pay_due_date,
    gross_amount,
    hold_flag,
    last_update_login,
    payment_method_lookup_code,
    payment_priority,
    payment_status_flag,
    second_discount_date,
    third_discount_date,
    batch_id,
    discount_amount_available,
    second_disc_amt_available,
    third_disc_amt_available,
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
    discount_amount_remaining,
    org_id,
    global_attribute_category,
    global_attribute1,
    global_attribute2,
    global_attribute3,
    global_attribute4,
    global_attribute5,
    global_attribute6,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.AP_PAYMENT_SCHEDULES_ALL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (INVOICE_ID, PAYMENT_NUM, kca_seq_id) IN (
      SELECT
        INVOICE_ID,
        PAYMENT_NUM,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.AP_PAYMENT_SCHEDULES_ALL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        INVOICE_ID,
        PAYMENT_NUM
    )
);
/* Soft delete */
UPDATE silver_bec_ods.AP_PAYMENT_SCHEDULES_ALL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.AP_PAYMENT_SCHEDULES_ALL SET IS_DELETED_FLG = 'Y'
WHERE
  (INVOICE_ID, PAYMENT_NUM) IN (
    SELECT
      INVOICE_ID,
      PAYMENT_NUM
    FROM bec_raw_dl_ext.AP_PAYMENT_SCHEDULES_ALL
    WHERE
      (INVOICE_ID, PAYMENT_NUM, KCA_SEQ_ID) IN (
        SELECT
          INVOICE_ID,
          PAYMENT_NUM,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.AP_PAYMENT_SCHEDULES_ALL
        GROUP BY
          INVOICE_ID,
          PAYMENT_NUM
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_payment_schedules_all';