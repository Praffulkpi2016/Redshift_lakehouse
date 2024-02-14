DROP table IF EXISTS silver_bec_ods.BOM_OPERATION_RESOURCES;
CREATE TABLE IF NOT EXISTS silver_bec_ods.bom_operation_resources (
  OPERATION_SEQUENCE_ID DECIMAL(15, 0),
  RESOURCE_SEQ_NUM DECIMAL(15, 0),
  RESOURCE_ID DECIMAL(15, 0),
  ACTIVITY_ID DECIMAL(15, 0),
  STANDARD_RATE_FLAG DECIMAL(15, 0),
  ASSIGNED_UNITS DECIMAL(28, 10),
  USAGE_RATE_OR_AMOUNT DECIMAL(28, 10),
  USAGE_RATE_OR_AMOUNT_INVERSE DECIMAL(28, 10),
  BASIS_TYPE DECIMAL(15, 0),
  SCHEDULE_FLAG DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  RESOURCE_OFFSET_PERCENT DECIMAL(28, 10),
  AUTOCHARGE_TYPE DECIMAL(15, 0),
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
  SCHEDULE_SEQ_NUM DECIMAL(15, 0),
  SUBSTITUTE_GROUP_NUM DECIMAL(15, 0),
  PRINCIPLE_FLAG DECIMAL(15, 0),
  SETUP_ID DECIMAL(15, 0),
  CHANGE_NOTICE STRING,
  ACD_TYPE DECIMAL(15, 0),
  ORIGINAL_SYSTEM_REFERENCE STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.bom_operation_resources (
  OPERATION_SEQUENCE_ID,
  RESOURCE_SEQ_NUM,
  RESOURCE_ID,
  ACTIVITY_ID,
  STANDARD_RATE_FLAG,
  ASSIGNED_UNITS,
  USAGE_RATE_OR_AMOUNT,
  USAGE_RATE_OR_AMOUNT_INVERSE,
  BASIS_TYPE,
  SCHEDULE_FLAG,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  RESOURCE_OFFSET_PERCENT,
  AUTOCHARGE_TYPE,
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
  SCHEDULE_SEQ_NUM,
  SUBSTITUTE_GROUP_NUM,
  PRINCIPLE_FLAG,
  SETUP_ID,
  CHANGE_NOTICE,
  ACD_TYPE,
  ORIGINAL_SYSTEM_REFERENCE,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    OPERATION_SEQUENCE_ID,
    RESOURCE_SEQ_NUM,
    RESOURCE_ID,
    ACTIVITY_ID,
    STANDARD_RATE_FLAG,
    ASSIGNED_UNITS,
    USAGE_RATE_OR_AMOUNT,
    USAGE_RATE_OR_AMOUNT_INVERSE,
    BASIS_TYPE,
    SCHEDULE_FLAG,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    RESOURCE_OFFSET_PERCENT,
    AUTOCHARGE_TYPE,
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
    SCHEDULE_SEQ_NUM,
    SUBSTITUTE_GROUP_NUM,
    PRINCIPLE_FLAG,
    SETUP_ID,
    CHANGE_NOTICE,
    ACD_TYPE,
    ORIGINAL_SYSTEM_REFERENCE,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.bom_operation_resources
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'bom_operation_resources';