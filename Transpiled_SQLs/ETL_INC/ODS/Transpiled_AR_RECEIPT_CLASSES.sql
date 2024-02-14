/* Delete Records */
DELETE FROM silver_bec_ods.AR_RECEIPT_CLASSES
WHERE
  receipt_class_id IN (
    SELECT
      stg.receipt_class_id
    FROM silver_bec_ods.AR_RECEIPT_CLASSES AS ods, bronze_bec_ods_stg.AR_RECEIPT_CLASSES AS stg
    WHERE
      ods.receipt_class_id = stg.receipt_class_id
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.AR_RECEIPT_CLASSES (
  receipt_class_id,
  confirm_flag,
  created_by,
  creation_date,
  creation_method_code,
  last_updated_by,
  last_update_date,
  clear_flag,
  `name`,
  remit_flag,
  creation_status,
  last_update_login,
  remit_method_code,
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
  notes_receivable,
  bill_of_exchange_flag,
  zd_edition_name,
  zd_sync,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    receipt_class_id,
    confirm_flag,
    created_by,
    creation_date,
    creation_method_code,
    last_updated_by,
    last_update_date,
    clear_flag,
    `name`,
    remit_flag,
    creation_status,
    last_update_login,
    remit_method_code,
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
    notes_receivable,
    bill_of_exchange_flag,
    zd_edition_name,
    zd_sync,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.AR_RECEIPT_CLASSES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (receipt_class_id, kca_seq_id) IN (
      SELECT
        receipt_class_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.AR_RECEIPT_CLASSES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        receipt_class_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.AR_RECEIPT_CLASSES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.AR_RECEIPT_CLASSES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    receipt_class_id
  ) IN (
    SELECT
      receipt_class_id
    FROM bec_raw_dl_ext.AR_RECEIPT_CLASSES
    WHERE
      (receipt_class_id, KCA_SEQ_ID) IN (
        SELECT
          receipt_class_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.AR_RECEIPT_CLASSES
        GROUP BY
          receipt_class_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ar_receipt_classes';