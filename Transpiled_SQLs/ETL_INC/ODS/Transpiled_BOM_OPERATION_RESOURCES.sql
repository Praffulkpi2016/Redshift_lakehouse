/* Delete Records */
DELETE FROM silver_bec_ods.BOM_OPERATION_RESOURCES
WHERE
  (COALESCE(OPERATION_SEQUENCE_ID, 0), COALESCE(RESOURCE_SEQ_NUM, 0)) IN (
    SELECT
      COALESCE(stg.OPERATION_SEQUENCE_ID, 0) AS OPERATION_SEQUENCE_ID,
      COALESCE(stg.RESOURCE_SEQ_NUM, 0) AS RESOURCE_SEQ_NUM
    FROM silver_bec_ods.bom_operation_resources AS ods, bronze_bec_ods_stg.bom_operation_resources AS stg
    WHERE
      COALESCE(ods.OPERATION_SEQUENCE_ID, 0) = COALESCE(stg.OPERATION_SEQUENCE_ID, 0)
      AND COALESCE(ods.RESOURCE_SEQ_NUM, 0) = COALESCE(stg.RESOURCE_SEQ_NUM, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
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
  IS_DELETED_FLG,
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
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(OPERATION_SEQUENCE_ID, 0), COALESCE(RESOURCE_SEQ_NUM, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(OPERATION_SEQUENCE_ID, 0) AS OPERATION_SEQUENCE_ID,
        COALESCE(RESOURCE_SEQ_NUM, 0) AS RESOURCE_SEQ_NUM,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.bom_operation_resources
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        OPERATION_SEQUENCE_ID,
        RESOURCE_SEQ_NUM
    )
);
/* Soft delete */
UPDATE silver_bec_ods.bom_operation_resources SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.bom_operation_resources SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(OPERATION_SEQUENCE_ID, 0), COALESCE(RESOURCE_SEQ_NUM, 0)) IN (
    SELECT
      COALESCE(OPERATION_SEQUENCE_ID, 0),
      COALESCE(RESOURCE_SEQ_NUM, 0)
    FROM bec_raw_dl_ext.bom_operation_resources
    WHERE
      (COALESCE(OPERATION_SEQUENCE_ID, 0), COALESCE(RESOURCE_SEQ_NUM, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(OPERATION_SEQUENCE_ID, 0),
          COALESCE(RESOURCE_SEQ_NUM, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.bom_operation_resources
        GROUP BY
          COALESCE(OPERATION_SEQUENCE_ID, 0),
          COALESCE(RESOURCE_SEQ_NUM, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'bom_operation_resources';