/* Delete Records */
DELETE FROM silver_bec_ods.XLA_AE_LINES
WHERE
  (AE_HEADER_ID, AE_LINE_NUM, APPLICATION_ID) IN (
    SELECT
      stg.AE_HEADER_ID,
      stg.AE_LINE_NUM,
      stg.APPLICATION_ID
    FROM silver_bec_ods.XLA_AE_LINES AS ods, bronze_bec_ods_stg.XLA_AE_LINES AS stg
    WHERE
      ods.AE_HEADER_ID = stg.AE_HEADER_ID
      AND ods.AE_LINE_NUM = stg.AE_LINE_NUM
      AND ods.APPLICATION_ID = stg.APPLICATION_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.XLA_AE_LINES (
  AE_HEADER_ID,
  AE_LINE_NUM,
  APPLICATION_ID,
  CODE_COMBINATION_ID,
  GL_TRANSFER_MODE_CODE,
  GL_SL_LINK_ID,
  ACCOUNTING_CLASS_CODE,
  PARTY_ID,
  PARTY_SITE_ID,
  PARTY_TYPE_CODE,
  ENTERED_DR,
  ENTERED_CR,
  ACCOUNTED_DR,
  ACCOUNTED_CR,
  DESCRIPTION,
  STATISTICAL_AMOUNT,
  CURRENCY_CODE,
  CURRENCY_CONVERSION_DATE,
  CURRENCY_CONVERSION_RATE,
  CURRENCY_CONVERSION_TYPE,
  USSGL_TRANSACTION_CODE,
  JGZZ_RECON_REF,
  CONTROL_BALANCE_FLAG,
  ANALYTICAL_BALANCE_FLAG,
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
  GL_SL_LINK_TABLE,
  DISPLAYED_LINE_NUMBER,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  PROGRAM_UPDATE_DATE,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  REQUEST_ID,
  UPG_BATCH_ID,
  UPG_TAX_REFERENCE_ID1,
  UPG_TAX_REFERENCE_ID2,
  UPG_TAX_REFERENCE_ID3,
  UNROUNDED_ACCOUNTED_DR,
  UNROUNDED_ACCOUNTED_CR,
  GAIN_OR_LOSS_FLAG,
  UNROUNDED_ENTERED_DR,
  UNROUNDED_ENTERED_CR,
  SUBSTITUTED_CCID,
  BUSINESS_CLASS_CODE,
  MPA_ACCRUAL_ENTRY_FLAG,
  ENCUMBRANCE_TYPE_ID,
  FUNDS_STATUS_CODE,
  MERGE_CODE_COMBINATION_ID,
  MERGE_PARTY_ID,
  MERGE_PARTY_SITE_ID,
  ACCOUNTING_DATE,
  LEDGER_ID,
  SOURCE_TABLE,
  SOURCE_ID,
  ACCOUNT_OVERLAY_SOURCE_ID,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    AE_HEADER_ID,
    AE_LINE_NUM,
    APPLICATION_ID,
    CODE_COMBINATION_ID,
    GL_TRANSFER_MODE_CODE,
    GL_SL_LINK_ID,
    ACCOUNTING_CLASS_CODE,
    PARTY_ID,
    PARTY_SITE_ID,
    PARTY_TYPE_CODE,
    ENTERED_DR,
    ENTERED_CR,
    ACCOUNTED_DR,
    ACCOUNTED_CR,
    DESCRIPTION,
    STATISTICAL_AMOUNT,
    CURRENCY_CODE,
    CURRENCY_CONVERSION_DATE,
    CURRENCY_CONVERSION_RATE,
    CURRENCY_CONVERSION_TYPE,
    USSGL_TRANSACTION_CODE,
    JGZZ_RECON_REF,
    CONTROL_BALANCE_FLAG,
    ANALYTICAL_BALANCE_FLAG,
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
    GL_SL_LINK_TABLE,
    DISPLAYED_LINE_NUMBER,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    PROGRAM_UPDATE_DATE,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    REQUEST_ID,
    UPG_BATCH_ID,
    UPG_TAX_REFERENCE_ID1,
    UPG_TAX_REFERENCE_ID2,
    UPG_TAX_REFERENCE_ID3,
    UNROUNDED_ACCOUNTED_DR,
    UNROUNDED_ACCOUNTED_CR,
    GAIN_OR_LOSS_FLAG,
    UNROUNDED_ENTERED_DR,
    UNROUNDED_ENTERED_CR,
    SUBSTITUTED_CCID,
    BUSINESS_CLASS_CODE,
    MPA_ACCRUAL_ENTRY_FLAG,
    ENCUMBRANCE_TYPE_ID,
    FUNDS_STATUS_CODE,
    MERGE_CODE_COMBINATION_ID,
    MERGE_PARTY_ID,
    MERGE_PARTY_SITE_ID,
    ACCOUNTING_DATE,
    LEDGER_ID,
    SOURCE_TABLE,
    SOURCE_ID,
    ACCOUNT_OVERLAY_SOURCE_ID,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.XLA_AE_LINES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (AE_HEADER_ID, AE_LINE_NUM, APPLICATION_ID, kca_seq_id) IN (
      SELECT
        AE_HEADER_ID,
        AE_LINE_NUM,
        APPLICATION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.XLA_AE_LINES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        AE_HEADER_ID,
        AE_LINE_NUM,
        APPLICATION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.XLA_AE_LINES SET IS_DELETED_FLG = 'Y'
WHERE
  (AE_HEADER_ID, AE_LINE_NUM, APPLICATION_ID) IN (
    SELECT
      ext.AE_HEADER_ID,
      ext.AE_LINE_NUM,
      ext.APPLICATION_ID
    FROM silver_bec_ods.XLA_AE_LINES AS ods, bec_raw_dl_ext.XLA_AE_LINES AS ext
    WHERE
      ods.AE_HEADER_ID = ext.AE_HEADER_ID
      AND ods.AE_LINE_NUM = ext.AE_LINE_NUM
      AND ods.APPLICATION_ID = ext.APPLICATION_ID
      AND ext.kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP(), load_type = 'I'
WHERE
  ods_table_name = 'xla_ae_lines';