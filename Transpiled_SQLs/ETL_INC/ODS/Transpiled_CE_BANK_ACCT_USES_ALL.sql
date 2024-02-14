/* Delete Records */
DELETE FROM silver_bec_ods.CE_BANK_ACCT_USES_ALL
WHERE
  BANK_ACCT_USE_ID IN (
    SELECT
      stg.BANK_ACCT_USE_ID
    FROM silver_bec_ods.CE_BANK_ACCT_USES_ALL AS ods, bronze_bec_ods_stg.CE_BANK_ACCT_USES_ALL AS stg
    WHERE
      ods.BANK_ACCT_USE_ID = stg.BANK_ACCT_USE_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
INSERT INTO silver_bec_ods.CE_BANK_ACCT_USES_ALL (
  BANK_ACCT_USE_ID,
  BANK_ACCOUNT_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  PRIMARY_FLAG,
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
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  ORG_ID,
  ORG_PARTY_ID,
  AP_USE_ENABLE_FLAG,
  AR_USE_ENABLE_FLAG,
  XTR_USE_ENABLE_FLAG,
  PAY_USE_ENABLE_FLAG,
  EDISC_RECEIVABLES_TRX_ID,
  UNEDISC_RECEIVABLES_TRX_ID,
  POOLED_FLAG,
  END_DATE,
  BR_STD_RECEIVABLES_TRX_ID,
  PAYMENT_DOC_CATEGORY,
  LEGAL_ENTITY_ID,
  INVESTMENT_LIMIT_CODE,
  FUNDING_LIMIT_CODE,
  AP_DEFAULT_SETTLEMENT_FLAG,
  XTR_DEFAULT_SETTLEMENT_FLAG,
  PAYROLL_BANK_ACCOUNT_ID,
  PRICING_MODEL,
  AUTHORIZED_FLAG,
  EFT_SCRIPT_NAME,
  PORTFOLIO_CODE,
  DEFAULT_ACCOUNT_FLAG,
  OBJECT_VERSION_NUMBER,
  AR_CLAIM_INV_ACT_ID,
  NEW_AR_RCPTS_RECEIVABLE_TRX_ID,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  BANK_ACCT_USE_ID,
  BANK_ACCOUNT_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  PRIMARY_FLAG,
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
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  ORG_ID,
  ORG_PARTY_ID,
  AP_USE_ENABLE_FLAG,
  AR_USE_ENABLE_FLAG,
  XTR_USE_ENABLE_FLAG,
  PAY_USE_ENABLE_FLAG,
  EDISC_RECEIVABLES_TRX_ID,
  UNEDISC_RECEIVABLES_TRX_ID,
  POOLED_FLAG,
  END_DATE,
  BR_STD_RECEIVABLES_TRX_ID,
  PAYMENT_DOC_CATEGORY,
  LEGAL_ENTITY_ID,
  INVESTMENT_LIMIT_CODE,
  FUNDING_LIMIT_CODE,
  AP_DEFAULT_SETTLEMENT_FLAG,
  XTR_DEFAULT_SETTLEMENT_FLAG,
  PAYROLL_BANK_ACCOUNT_ID,
  PRICING_MODEL,
  AUTHORIZED_FLAG,
  EFT_SCRIPT_NAME,
  PORTFOLIO_CODE,
  DEFAULT_ACCOUNT_FLAG,
  OBJECT_VERSION_NUMBER,
  AR_CLAIM_INV_ACT_ID,
  NEW_AR_RCPTS_RECEIVABLE_TRX_ID,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.CE_BANK_ACCT_USES_ALL
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (BANK_ACCT_USE_ID, kca_seq_id) IN (
    SELECT
      BANK_ACCT_USE_ID,
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.CE_BANK_ACCT_USES_ALL
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      BANK_ACCT_USE_ID
  );
/* Soft Delete */
UPDATE silver_bec_ods.CE_BANK_ACCT_USES_ALL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.CE_BANK_ACCT_USES_ALL SET IS_DELETED_FLG = 'Y'
WHERE
  (
    BANK_ACCT_USE_ID
  ) IN (
    SELECT
      BANK_ACCT_USE_ID
    FROM bec_raw_dl_ext.CE_BANK_ACCT_USES_ALL
    WHERE
      (BANK_ACCT_USE_ID, KCA_SEQ_ID) IN (
        SELECT
          BANK_ACCT_USE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.CE_BANK_ACCT_USES_ALL
        GROUP BY
          BANK_ACCT_USE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ce_bank_acct_uses_all';