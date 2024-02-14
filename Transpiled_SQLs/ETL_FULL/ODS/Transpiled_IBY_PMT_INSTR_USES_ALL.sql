DROP TABLE IF EXISTS silver_bec_ods.IBY_PMT_INSTR_USES_ALL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.IBY_PMT_INSTR_USES_ALL (
  INSTRUMENT_PAYMENT_USE_ID DECIMAL(15, 0),
  PAYMENT_FLOW STRING,
  EXT_PMT_PARTY_ID DECIMAL(15, 0),
  INSTRUMENT_TYPE STRING,
  INSTRUMENT_ID DECIMAL(15, 0),
  PAYMENT_FUNCTION STRING,
  ORDER_OF_PREFERENCE DECIMAL(15, 0),
  CREATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  OBJECT_VERSION_NUMBER DECIMAL(28, 10),
  START_DATE TIMESTAMP,
  END_DATE TIMESTAMP,
  DEBIT_AUTH_FLAG STRING,
  DEBIT_AUTH_METHOD STRING,
  DEBIT_AUTH_REFERENCE STRING,
  DEBIT_AUTH_BEGIN TIMESTAMP,
  DEBIT_AUTH_END TIMESTAMP,
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
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.IBY_PMT_INSTR_USES_ALL (
  INSTRUMENT_PAYMENT_USE_ID,
  PAYMENT_FLOW,
  EXT_PMT_PARTY_ID,
  INSTRUMENT_TYPE,
  INSTRUMENT_ID,
  PAYMENT_FUNCTION,
  ORDER_OF_PREFERENCE,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  OBJECT_VERSION_NUMBER,
  START_DATE,
  END_DATE,
  DEBIT_AUTH_FLAG,
  DEBIT_AUTH_METHOD,
  DEBIT_AUTH_REFERENCE,
  DEBIT_AUTH_BEGIN,
  DEBIT_AUTH_END,
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
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  INSTRUMENT_PAYMENT_USE_ID,
  PAYMENT_FLOW,
  EXT_PMT_PARTY_ID,
  INSTRUMENT_TYPE,
  INSTRUMENT_ID,
  PAYMENT_FUNCTION,
  ORDER_OF_PREFERENCE,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  OBJECT_VERSION_NUMBER,
  START_DATE,
  END_DATE,
  DEBIT_AUTH_FLAG,
  DEBIT_AUTH_METHOD,
  DEBIT_AUTH_REFERENCE,
  DEBIT_AUTH_BEGIN,
  DEBIT_AUTH_END,
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
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF('', '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.IBY_PMT_INSTR_USES_ALL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'iby_pmt_instr_uses_all';