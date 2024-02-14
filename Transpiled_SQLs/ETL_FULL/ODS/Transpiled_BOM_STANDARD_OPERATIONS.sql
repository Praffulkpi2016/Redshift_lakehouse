DROP table IF EXISTS silver_bec_ods.BOM_STANDARD_OPERATIONS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.bom_standard_operations (
  SEQUENCE_NUM DECIMAL(15, 0),
  LINE_ID DECIMAL(15, 0),
  OPERATION_TYPE DECIMAL(15, 0),
  STANDARD_OPERATION_ID DECIMAL(15, 0),
  OPERATION_CODE STRING,
  ORGANIZATION_ID DECIMAL(15, 0),
  DEPARTMENT_ID DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  MINIMUM_TRANSFER_QUANTITY DECIMAL(28, 10),
  COUNT_POINT_TYPE DECIMAL(15, 0),
  OPERATION_DESCRIPTION STRING,
  OPTION_DEPENDENT_FLAG DECIMAL(15, 0),
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
  BACKFLUSH_FLAG DECIMAL(15, 0),
  WMS_TASK_TYPE DECIMAL(15, 0),
  INCLUDE_IN_ROLLUP DECIMAL(28, 10),
  YIELD DECIMAL(28, 10),
  OPERATION_YIELD_ENABLED DECIMAL(15, 0),
  SHUTDOWN_TYPE STRING,
  ACTUAL_IPK DECIMAL(28, 10),
  CRITICAL_TO_QUALITY STRING,
  VALUE_ADDED STRING,
  DEFAULT_SUBINVENTORY STRING,
  DEFAULT_LOCATOR_ID DECIMAL(15, 0),
  LOWEST_ACCEPTABLE_YIELD DECIMAL(28, 10),
  USE_ORG_SETTINGS DECIMAL(15, 0),
  QUEUE_MANDATORY_FLAG DECIMAL(15, 0),
  RUN_MANDATORY_FLAG DECIMAL(15, 0),
  TO_MOVE_MANDATORY_FLAG DECIMAL(15, 0),
  SHOW_NEXT_OP_BY_DEFAULT DECIMAL(15, 0),
  SHOW_SCRAP_CODE DECIMAL(15, 0),
  SHOW_LOT_ATTRIB DECIMAL(15, 0),
  TRACK_MULTIPLE_RES_USAGE_DATES DECIMAL(15, 0),
  CHECK_SKILL DECIMAL(15, 0),
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.bom_standard_operations (
  SEQUENCE_NUM,
  LINE_ID,
  OPERATION_TYPE,
  STANDARD_OPERATION_ID,
  OPERATION_CODE,
  ORGANIZATION_ID,
  DEPARTMENT_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  MINIMUM_TRANSFER_QUANTITY,
  COUNT_POINT_TYPE,
  OPERATION_DESCRIPTION,
  OPTION_DEPENDENT_FLAG,
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
  BACKFLUSH_FLAG,
  WMS_TASK_TYPE,
  INCLUDE_IN_ROLLUP,
  YIELD,
  OPERATION_YIELD_ENABLED,
  SHUTDOWN_TYPE,
  ACTUAL_IPK,
  CRITICAL_TO_QUALITY,
  VALUE_ADDED,
  DEFAULT_SUBINVENTORY,
  DEFAULT_LOCATOR_ID,
  LOWEST_ACCEPTABLE_YIELD,
  USE_ORG_SETTINGS,
  QUEUE_MANDATORY_FLAG,
  RUN_MANDATORY_FLAG,
  TO_MOVE_MANDATORY_FLAG,
  SHOW_NEXT_OP_BY_DEFAULT,
  SHOW_SCRAP_CODE,
  SHOW_LOT_ATTRIB,
  TRACK_MULTIPLE_RES_USAGE_DATES,
  CHECK_SKILL,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    SEQUENCE_NUM,
    LINE_ID,
    OPERATION_TYPE,
    STANDARD_OPERATION_ID,
    OPERATION_CODE,
    ORGANIZATION_ID,
    DEPARTMENT_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    MINIMUM_TRANSFER_QUANTITY,
    COUNT_POINT_TYPE,
    OPERATION_DESCRIPTION,
    OPTION_DEPENDENT_FLAG,
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
    BACKFLUSH_FLAG,
    WMS_TASK_TYPE,
    INCLUDE_IN_ROLLUP,
    YIELD,
    OPERATION_YIELD_ENABLED,
    SHUTDOWN_TYPE,
    ACTUAL_IPK,
    CRITICAL_TO_QUALITY,
    VALUE_ADDED,
    DEFAULT_SUBINVENTORY,
    DEFAULT_LOCATOR_ID,
    LOWEST_ACCEPTABLE_YIELD,
    USE_ORG_SETTINGS,
    QUEUE_MANDATORY_FLAG,
    RUN_MANDATORY_FLAG,
    TO_MOVE_MANDATORY_FLAG,
    SHOW_NEXT_OP_BY_DEFAULT,
    SHOW_SCRAP_CODE,
    SHOW_LOT_ATTRIB,
    TRACK_MULTIPLE_RES_USAGE_DATES,
    CHECK_SKILL,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.bom_standard_operations
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'bom_standard_operations';