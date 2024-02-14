/* Delete Records */
DELETE FROM silver_bec_ods.AP_CHECK_FORMATS
WHERE
  check_format_id IN (
    SELECT
      stg.check_format_id
    FROM silver_bec_ods.AP_CHECK_FORMATS AS ods, bronze_bec_ods_stg.AP_CHECK_FORMATS AS stg
    WHERE
      ods.check_format_id = stg.check_format_id
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.AP_CHECK_FORMATS (
  CHECK_FORMAT_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  NAME,
  TYPE,
  STUB_FIRST_FLAG,
  CHECK_LENGTH,
  STUB_LENGTH,
  INVOICES_PER_STUB,
  OVERFLOW_RULE,
  PRE_NUMBERED_FLAG,
  CHECK_NUM_ROW_LINE,
  DATE_ROW_LINE,
  AMOUNT_ROW_LINE,
  AMOUNT_WORDS_ROW_LINE,
  VENDOR_NUM_ROW_LINE,
  VENDOR_NAME_ROW_LINE,
  VENDOR_ADDRESS_ROW_LINE,
  STUB_CHECK_NUM_ROW_LINE,
  STUB_VENDOR_NAME_ROW_LINE,
  STUB_VENDOR_NUM_ROW_LINE,
  STUB_FIRST_INVOICE_ROW_LINE,
  PAYMENT_METHOD_LOOKUP_CODE,
  CURRENCY_CODE,
  CREATE_PAYMENTS_PROGRAM_ID,
  CONFIRM_PAYMENTS_PROGRAM_ID,
  SEPARATE_REMITTANCE_ADVICE,
  REMITTANCE_ADVICE_PROGRAM_ID,
  LAST_UPDATE_LOGIN,
  CREATION_DATE,
  CREATED_BY,
  EFT_TYPE,
  MULTI_CURRENCY_FLAG,
  ATTRIBUTE_CATEGORY,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  ZERO_AMOUNTS_ONLY,
  BUILD_PAYMENTS_PROGRAM_ID,
  FORMAT_PAYMENTS_PROGRAM_ID,
  PRINT_CHECK_STUB,
  GROUP_BY_DUE_DATE,
  GLOBAL_ATTRIBUTE_CATEGORY,
  GLOBAL_ATTRIBUTE1,
  GLOBAL_ATTRIBUTE2,
  GLOBAL_ATTRIBUTE3,
  GLOBAL_ATTRIBUTE4,
  GLOBAL_ATTRIBUTE5,
  GLOBAL_ATTRIBUTE6,
  GLOBAL_ATTRIBUTE7,
  GLOBAL_ATTRIBUTE8,
  GLOBAL_ATTRIBUTE9,
  GLOBAL_ATTRIBUTE10,
  GLOBAL_ATTRIBUTE11,
  GLOBAL_ATTRIBUTE12,
  GLOBAL_ATTRIBUTE13,
  GLOBAL_ATTRIBUTE14,
  GLOBAL_ATTRIBUTE15,
  GLOBAL_ATTRIBUTE16,
  GLOBAL_ATTRIBUTE17,
  GLOBAL_ATTRIBUTE18,
  GLOBAL_ATTRIBUTE19,
  GLOBAL_ATTRIBUTE20,
  BANK_FILE_CHARACTER_SET,
  TRANSMISSIONS_FLAG,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    CHECK_FORMAT_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    NAME,
    TYPE,
    STUB_FIRST_FLAG,
    CHECK_LENGTH,
    STUB_LENGTH,
    INVOICES_PER_STUB,
    OVERFLOW_RULE,
    PRE_NUMBERED_FLAG,
    CHECK_NUM_ROW_LINE,
    DATE_ROW_LINE,
    AMOUNT_ROW_LINE,
    AMOUNT_WORDS_ROW_LINE,
    VENDOR_NUM_ROW_LINE,
    VENDOR_NAME_ROW_LINE,
    VENDOR_ADDRESS_ROW_LINE,
    STUB_CHECK_NUM_ROW_LINE,
    STUB_VENDOR_NAME_ROW_LINE,
    STUB_VENDOR_NUM_ROW_LINE,
    STUB_FIRST_INVOICE_ROW_LINE,
    PAYMENT_METHOD_LOOKUP_CODE,
    CURRENCY_CODE,
    CREATE_PAYMENTS_PROGRAM_ID,
    CONFIRM_PAYMENTS_PROGRAM_ID,
    SEPARATE_REMITTANCE_ADVICE,
    REMITTANCE_ADVICE_PROGRAM_ID,
    LAST_UPDATE_LOGIN,
    CREATION_DATE,
    CREATED_BY,
    EFT_TYPE,
    MULTI_CURRENCY_FLAG,
    ATTRIBUTE_CATEGORY,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    ATTRIBUTE6,
    ATTRIBUTE7,
    ATTRIBUTE8,
    ATTRIBUTE9,
    ATTRIBUTE10,
    ATTRIBUTE11,
    ATTRIBUTE12,
    ATTRIBUTE13,
    ATTRIBUTE14,
    ATTRIBUTE15,
    ZERO_AMOUNTS_ONLY,
    BUILD_PAYMENTS_PROGRAM_ID,
    FORMAT_PAYMENTS_PROGRAM_ID,
    PRINT_CHECK_STUB,
    GROUP_BY_DUE_DATE,
    GLOBAL_ATTRIBUTE_CATEGORY,
    GLOBAL_ATTRIBUTE1,
    GLOBAL_ATTRIBUTE2,
    GLOBAL_ATTRIBUTE3,
    GLOBAL_ATTRIBUTE4,
    GLOBAL_ATTRIBUTE5,
    GLOBAL_ATTRIBUTE6,
    GLOBAL_ATTRIBUTE7,
    GLOBAL_ATTRIBUTE8,
    GLOBAL_ATTRIBUTE9,
    GLOBAL_ATTRIBUTE10,
    GLOBAL_ATTRIBUTE11,
    GLOBAL_ATTRIBUTE12,
    GLOBAL_ATTRIBUTE13,
    GLOBAL_ATTRIBUTE14,
    GLOBAL_ATTRIBUTE15,
    GLOBAL_ATTRIBUTE16,
    GLOBAL_ATTRIBUTE17,
    GLOBAL_ATTRIBUTE18,
    GLOBAL_ATTRIBUTE19,
    GLOBAL_ATTRIBUTE20,
    BANK_FILE_CHARACTER_SET,
    TRANSMISSIONS_FLAG,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.AP_CHECK_FORMATS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (check_format_id, kca_seq_id) IN (
      SELECT
        check_format_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.AP_CHECK_FORMATS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        check_format_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.AP_CHECK_FORMATS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.AP_CHECK_FORMATS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    check_format_id
  ) IN (
    SELECT
      check_format_id
    FROM bec_raw_dl_ext.AP_CHECK_FORMATS
    WHERE
      (check_format_id, KCA_SEQ_ID) IN (
        SELECT
          check_format_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.AP_CHECK_FORMATS
        GROUP BY
          check_format_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_check_formats';