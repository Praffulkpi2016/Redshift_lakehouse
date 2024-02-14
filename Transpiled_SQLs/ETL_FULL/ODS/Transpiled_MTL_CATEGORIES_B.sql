DROP table IF EXISTS silver_bec_ods.MTL_CATEGORIES_B;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MTL_CATEGORIES_B (
  CATEGORY_ID DECIMAL(15, 0),
  STRUCTURE_ID DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  DESCRIPTION STRING,
  DISABLE_DATE TIMESTAMP,
  SEGMENT1 STRING,
  SEGMENT2 STRING,
  SEGMENT3 STRING,
  SEGMENT4 STRING,
  SEGMENT5 STRING,
  SEGMENT6 STRING,
  SEGMENT7 STRING,
  SEGMENT8 STRING,
  SEGMENT9 STRING,
  SEGMENT10 STRING,
  SEGMENT11 STRING,
  SEGMENT12 STRING,
  SEGMENT13 STRING,
  SEGMENT14 STRING,
  SEGMENT15 STRING,
  SEGMENT16 STRING,
  SEGMENT17 STRING,
  SEGMENT18 STRING,
  SEGMENT19 STRING,
  SEGMENT20 STRING,
  SUMMARY_FLAG STRING,
  ENABLED_FLAG STRING,
  START_DATE_ACTIVE TIMESTAMP,
  END_DATE_ACTIVE TIMESTAMP,
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
  REQUEST_ID DECIMAL(15, 0),
  PROGRAM_APPLICATION_ID DECIMAL(15, 0),
  PROGRAM_ID DECIMAL(15, 0),
  PROGRAM_UPDATE_DATE TIMESTAMP,
  WEB_STATUS STRING,
  SUPPLIER_ENABLED_FLAG STRING,
  ZD_EDITION_NAME STRING,
  ZD_SYNC STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.MTL_CATEGORIES_B (
  CATEGORY_ID,
  STRUCTURE_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  DISABLE_DATE,
  SEGMENT1,
  SEGMENT2,
  SEGMENT3,
  SEGMENT4,
  SEGMENT5,
  SEGMENT6,
  SEGMENT7,
  SEGMENT8,
  SEGMENT9,
  SEGMENT10,
  SEGMENT11,
  SEGMENT12,
  SEGMENT13,
  SEGMENT14,
  SEGMENT15,
  SEGMENT16,
  SEGMENT17,
  SEGMENT18,
  SEGMENT19,
  SEGMENT20,
  SUMMARY_FLAG,
  ENABLED_FLAG,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
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
  PROGRAM_UPDATE_DATE, /* ,WH_UPDATE_DATE */ /* ,TOTAL_PROD_ID */
  WEB_STATUS,
  SUPPLIER_ENABLED_FLAG,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    CATEGORY_ID,
    STRUCTURE_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    DISABLE_DATE,
    SEGMENT1,
    SEGMENT2,
    SEGMENT3,
    SEGMENT4,
    SEGMENT5,
    SEGMENT6,
    SEGMENT7,
    SEGMENT8,
    SEGMENT9,
    SEGMENT10,
    SEGMENT11,
    SEGMENT12,
    SEGMENT13,
    SEGMENT14,
    SEGMENT15,
    SEGMENT16,
    SEGMENT17,
    SEGMENT18,
    SEGMENT19,
    SEGMENT20,
    SUMMARY_FLAG,
    ENABLED_FLAG,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
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
    PROGRAM_UPDATE_DATE, /* ,WH_UPDATE_DATE */ /* ,TOTAL_PROD_ID */
    WEB_STATUS,
    SUPPLIER_ENABLED_FLAG,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_CATEGORIES_B
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_categories_b';