DROP table IF EXISTS silver_bec_ods.GL_JE_BATCHES;
CREATE TABLE IF NOT EXISTS silver_bec_ods.GL_JE_BATCHES (
  JE_BATCH_ID DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  SET_OF_BOOKS_ID_11I DECIMAL(15, 0),
  NAME STRING,
  STATUS STRING,
  STATUS_VERIFIED STRING,
  ACTUAL_FLAG STRING,
  DEFAULT_EFFECTIVE_DATE TIMESTAMP,
  BUDGETARY_CONTROL_STATUS STRING,
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  STATUS_RESET_FLAG STRING,
  DEFAULT_PERIOD_NAME STRING,
  UNIQUE_DATE STRING,
  EARLIEST_POSTABLE_DATE TIMESTAMP,
  POSTED_DATE TIMESTAMP,
  DATE_CREATED TIMESTAMP,
  DESCRIPTION STRING,
  CONTROL_TOTAL DECIMAL(28, 10),
  RUNNING_TOTAL_DR DECIMAL(28, 10),
  RUNNING_TOTAL_CR DECIMAL(28, 10),
  RUNNING_TOTAL_ACCOUNTED_DR DECIMAL(28, 10),
  RUNNING_TOTAL_ACCOUNTED_CR DECIMAL(28, 10),
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
  CONTEXT STRING,
  PACKET_ID DECIMAL(15, 0),
  USSGL_TRANSACTION_CODE STRING,
  CONTEXT2 STRING,
  POSTING_RUN_ID DECIMAL(15, 0),
  REQUEST_ID DECIMAL(15, 0),
  UNRESERVATION_PACKET_ID DECIMAL(15, 0),
  AVERAGE_JOURNAL_FLAG STRING,
  ORG_ID DECIMAL(15, 0),
  APPROVAL_STATUS_CODE STRING,
  PARENT_JE_BATCH_ID DECIMAL(15, 0),
  POSTED_BY DECIMAL(15, 0),
  CHART_OF_ACCOUNTS_ID DECIMAL(15, 0),
  PERIOD_SET_NAME STRING,
  ACCOUNTED_PERIOD_TYPE STRING,
  GROUP_ID DECIMAL(15, 0),
  APPROVER_EMPLOYEE_ID DECIMAL(15, 0),
  GLOBAL_ATTRIBUTE_CATEGORY STRING,
  GLOBAL_ATTRIBUTE1 STRING,
  GLOBAL_ATTRIBUTE2 STRING,
  GLOBAL_ATTRIBUTE3 STRING,
  GLOBAL_ATTRIBUTE4 STRING,
  GLOBAL_ATTRIBUTE5 STRING,
  GLOBAL_ATTRIBUTE6 STRING,
  GLOBAL_ATTRIBUTE7 STRING,
  GLOBAL_ATTRIBUTE8 STRING,
  GLOBAL_ATTRIBUTE9 STRING,
  GLOBAL_ATTRIBUTE10 STRING,
  GLOBAL_ATTRIBUTE11 STRING,
  GLOBAL_ATTRIBUTE12 STRING,
  GLOBAL_ATTRIBUTE13 STRING,
  GLOBAL_ATTRIBUTE14 STRING,
  GLOBAL_ATTRIBUTE15 STRING,
  GLOBAL_ATTRIBUTE16 STRING,
  GLOBAL_ATTRIBUTE17 STRING,
  GLOBAL_ATTRIBUTE18 STRING,
  GLOBAL_ATTRIBUTE19 STRING,
  GLOBAL_ATTRIBUTE20 STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.GL_JE_BATCHES (
  JE_BATCH_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  SET_OF_BOOKS_ID_11I,
  NAME,
  STATUS,
  STATUS_VERIFIED,
  ACTUAL_FLAG,
  DEFAULT_EFFECTIVE_DATE,
  BUDGETARY_CONTROL_STATUS,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  STATUS_RESET_FLAG,
  DEFAULT_PERIOD_NAME,
  UNIQUE_DATE,
  EARLIEST_POSTABLE_DATE,
  POSTED_DATE,
  DATE_CREATED,
  DESCRIPTION,
  CONTROL_TOTAL,
  RUNNING_TOTAL_DR,
  RUNNING_TOTAL_CR,
  RUNNING_TOTAL_ACCOUNTED_DR,
  RUNNING_TOTAL_ACCOUNTED_CR,
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
  CONTEXT,
  PACKET_ID,
  USSGL_TRANSACTION_CODE,
  CONTEXT2,
  POSTING_RUN_ID,
  REQUEST_ID,
  UNRESERVATION_PACKET_ID,
  AVERAGE_JOURNAL_FLAG,
  ORG_ID,
  APPROVAL_STATUS_CODE,
  PARENT_JE_BATCH_ID,
  POSTED_BY,
  CHART_OF_ACCOUNTS_ID,
  PERIOD_SET_NAME,
  ACCOUNTED_PERIOD_TYPE,
  GROUP_ID,
  APPROVER_EMPLOYEE_ID,
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
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    JE_BATCH_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    SET_OF_BOOKS_ID_11I,
    NAME,
    STATUS,
    STATUS_VERIFIED,
    ACTUAL_FLAG,
    DEFAULT_EFFECTIVE_DATE,
    BUDGETARY_CONTROL_STATUS,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    STATUS_RESET_FLAG,
    DEFAULT_PERIOD_NAME,
    UNIQUE_DATE,
    EARLIEST_POSTABLE_DATE,
    POSTED_DATE,
    DATE_CREATED,
    DESCRIPTION,
    CONTROL_TOTAL,
    RUNNING_TOTAL_DR,
    RUNNING_TOTAL_CR,
    RUNNING_TOTAL_ACCOUNTED_DR,
    RUNNING_TOTAL_ACCOUNTED_CR,
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
    CONTEXT,
    PACKET_ID,
    USSGL_TRANSACTION_CODE,
    CONTEXT2,
    POSTING_RUN_ID,
    REQUEST_ID,
    UNRESERVATION_PACKET_ID,
    AVERAGE_JOURNAL_FLAG,
    ORG_ID,
    APPROVAL_STATUS_CODE,
    PARENT_JE_BATCH_ID,
    POSTED_BY,
    CHART_OF_ACCOUNTS_ID,
    PERIOD_SET_NAME,
    ACCOUNTED_PERIOD_TYPE,
    GROUP_ID,
    APPROVER_EMPLOYEE_ID,
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.GL_JE_BATCHES
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_je_batches';