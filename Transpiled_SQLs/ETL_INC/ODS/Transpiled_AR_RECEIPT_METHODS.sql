/* Delete Records */
DELETE FROM silver_bec_ods.AR_RECEIPT_METHODS
WHERE
  receipt_method_id IN (
    SELECT
      stg.receipt_method_id
    FROM silver_bec_ods.AR_RECEIPT_METHODS AS ods, bronze_bec_ods_stg.AR_RECEIPT_METHODS AS stg
    WHERE
      ods.receipt_method_id = stg.receipt_method_id
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.AR_RECEIPT_METHODS (
  receipt_method_id,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  `name`,
  receipt_class_id,
  start_date,
  auto_print_program_id,
  auto_trans_program_id,
  end_date,
  last_update_login,
  lead_days,
  maturity_date_rule_code,
  receipt_creation_rule_code,
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
  printed_name,
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
  payment_type_code,
  merchant_id,
  receipt_inherit_inv_num_flag,
  dm_inherit_receipt_num_flag,
  br_cust_trx_type_id,
  br_min_acctd_amount,
  br_max_acctd_amount,
  br_inherit_inv_num_flag,
  merchant_ref,
  payment_channel_code,
  zd_edition_name,
  zd_sync,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    receipt_method_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    `name`,
    receipt_class_id,
    start_date,
    auto_print_program_id,
    auto_trans_program_id,
    end_date,
    last_update_login,
    lead_days,
    maturity_date_rule_code,
    receipt_creation_rule_code,
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
    printed_name,
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
    payment_type_code,
    merchant_id,
    receipt_inherit_inv_num_flag,
    dm_inherit_receipt_num_flag,
    br_cust_trx_type_id,
    br_min_acctd_amount,
    br_max_acctd_amount,
    br_inherit_inv_num_flag,
    merchant_ref,
    payment_channel_code,
    zd_edition_name,
    zd_sync,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.AR_RECEIPT_METHODS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (receipt_method_id, kca_seq_id) IN (
      SELECT
        receipt_method_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.AR_RECEIPT_METHODS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        receipt_method_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.AR_RECEIPT_METHODS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.AR_RECEIPT_METHODS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    receipt_method_id
  ) IN (
    SELECT
      receipt_method_id
    FROM bec_raw_dl_ext.AR_RECEIPT_METHODS
    WHERE
      (receipt_method_id, KCA_SEQ_ID) IN (
        SELECT
          receipt_method_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.AR_RECEIPT_METHODS
        GROUP BY
          receipt_method_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ar_receipt_methods';