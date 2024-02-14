/* Delete Records */
DELETE FROM silver_bec_ods.CE_PAYMENT_DOCUMENTS
WHERE
  PAYMENT_DOCUMENT_ID IN (
    SELECT
      stg.PAYMENT_DOCUMENT_ID
    FROM silver_bec_ods.CE_PAYMENT_DOCUMENTS AS ods, bronze_bec_ods_stg.CE_PAYMENT_DOCUMENTS AS stg
    WHERE
      ods.PAYMENT_DOCUMENT_ID = stg.PAYMENT_DOCUMENT_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
INSERT INTO silver_bec_ods.CE_PAYMENT_DOCUMENTS (
  PAYMENT_DOCUMENT_ID,
  PAYMENT_DOC_CATEGORY,
  PAYMENT_DOCUMENT_NAME,
  PAYMENT_INSTRUCTION_ID,
  INTERNAL_BANK_ACCOUNT_ID,
  PAPER_STOCK_TYPE,
  ATTACHED_REMITTANCE_STUB_FLAG,
  NUMBER_OF_LINES_PER_REMIT_STUB,
  NUMBER_OF_SETUP_DOCUMENTS,
  FORMAT_CODE,
  FIRST_AVAILABLE_DOCUMENT_NUM,
  LAST_AVAILABLE_DOCUMENT_NUMBER,
  LAST_ISSUED_DOCUMENT_NUMBER,
  MANUAL_PAYMENTS_ONLY_FLAG,
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
  INACTIVE_DATE,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  OBJECT_VERSION_NUMBER,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  PAYMENT_DOCUMENT_ID,
  PAYMENT_DOC_CATEGORY,
  PAYMENT_DOCUMENT_NAME,
  PAYMENT_INSTRUCTION_ID,
  INTERNAL_BANK_ACCOUNT_ID,
  PAPER_STOCK_TYPE,
  ATTACHED_REMITTANCE_STUB_FLAG,
  NUMBER_OF_LINES_PER_REMIT_STUB,
  NUMBER_OF_SETUP_DOCUMENTS,
  FORMAT_CODE,
  FIRST_AVAILABLE_DOCUMENT_NUM,
  LAST_AVAILABLE_DOCUMENT_NUMBER,
  LAST_ISSUED_DOCUMENT_NUMBER,
  MANUAL_PAYMENTS_ONLY_FLAG,
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
  INACTIVE_DATE,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  OBJECT_VERSION_NUMBER,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.CE_PAYMENT_DOCUMENTS
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (PAYMENT_DOCUMENT_ID, kca_seq_id) IN (
    SELECT
      PAYMENT_DOCUMENT_ID,
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.CE_PAYMENT_DOCUMENTS
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      PAYMENT_DOCUMENT_ID
  );
/* Soft Delete */
UPDATE silver_bec_ods.CE_PAYMENT_DOCUMENTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.CE_PAYMENT_DOCUMENTS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    PAYMENT_DOCUMENT_ID
  ) IN (
    SELECT
      PAYMENT_DOCUMENT_ID
    FROM bec_raw_dl_ext.CE_PAYMENT_DOCUMENTS
    WHERE
      (PAYMENT_DOCUMENT_ID, KCA_SEQ_ID) IN (
        SELECT
          PAYMENT_DOCUMENT_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.CE_PAYMENT_DOCUMENTS
        GROUP BY
          PAYMENT_DOCUMENT_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ce_payment_documents';