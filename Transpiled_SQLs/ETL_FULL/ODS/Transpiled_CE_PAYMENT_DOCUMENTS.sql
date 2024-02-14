DROP table IF EXISTS silver_bec_ods.CE_PAYMENT_DOCUMENTS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.CE_PAYMENT_DOCUMENTS (
  PAYMENT_DOCUMENT_ID DECIMAL(15, 0),
  PAYMENT_DOC_CATEGORY STRING,
  PAYMENT_DOCUMENT_NAME STRING,
  PAYMENT_INSTRUCTION_ID DECIMAL(15, 0),
  INTERNAL_BANK_ACCOUNT_ID DECIMAL(15, 0),
  PAPER_STOCK_TYPE STRING,
  ATTACHED_REMITTANCE_STUB_FLAG STRING,
  NUMBER_OF_LINES_PER_REMIT_STUB DECIMAL(15, 0),
  NUMBER_OF_SETUP_DOCUMENTS DECIMAL(15, 0),
  FORMAT_CODE STRING,
  FIRST_AVAILABLE_DOCUMENT_NUM DECIMAL(15, 0),
  LAST_AVAILABLE_DOCUMENT_NUMBER DECIMAL(15, 0),
  LAST_ISSUED_DOCUMENT_NUMBER DECIMAL(15, 0),
  MANUAL_PAYMENTS_ONLY_FLAG STRING,
  ATTRIBUTE_CATEGORY STRING,
  ATTRIBUTE1 STRING,
  ATTRIBUTE2 STRING,
  ATTRIBUTE3 STRING,
  ATTRIBUTE4 STRING,
  ATTRIBUTE5 STRING,
  ATTRIBUTE6 STRING,
  ATTRIBUTE7 STRING,
  ATTRIBUTE8 STRING,
  ATTRIBUTE9 STRING,
  ATTRIBUTE10 STRING,
  ATTRIBUTE11 STRING,
  ATTRIBUTE12 STRING,
  ATTRIBUTE13 STRING,
  ATTRIBUTE14 STRING,
  ATTRIBUTE15 STRING,
  INACTIVE_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  OBJECT_VERSION_NUMBER DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
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
FROM bronze_bec_ods_stg.CE_PAYMENT_DOCUMENTS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ce_payment_documents';