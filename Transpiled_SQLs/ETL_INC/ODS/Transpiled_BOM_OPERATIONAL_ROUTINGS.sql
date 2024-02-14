/* Delete Records */
DELETE FROM silver_bec_ods.BOM_OPERATIONAL_ROUTINGS
WHERE
  (
    COALESCE(ROUTING_SEQUENCE_ID, '0')
  ) IN (
    SELECT
      COALESCE(stg.ROUTING_SEQUENCE_ID, '0') AS ROUTING_SEQUENCE_ID
    FROM silver_bec_ods.bom_operational_routings AS ods, bronze_bec_ods_stg.bom_operational_routings AS stg
    WHERE
      COALESCE(ods.ROUTING_SEQUENCE_ID, '0') = COALESCE(stg.ROUTING_SEQUENCE_ID, '0')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.bom_operational_routings (
  ROUTING_SEQUENCE_ID,
  ASSEMBLY_ITEM_ID,
  ORGANIZATION_ID,
  ALTERNATE_ROUTING_DESIGNATOR,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  ROUTING_TYPE,
  COMMON_ASSEMBLY_ITEM_ID,
  COMMON_ROUTING_SEQUENCE_ID,
  ROUTING_COMMENT,
  COMPLETION_SUBINVENTORY,
  COMPLETION_LOCATOR_ID,
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
  LINE_ID,
  CFM_ROUTING_FLAG,
  MIXED_MODEL_MAP_FLAG,
  PRIORITY,
  TOTAL_PRODUCT_CYCLE_TIME,
  CTP_FLAG,
  PROJECT_ID,
  TASK_ID,
  PENDING_FROM_ECN,
  ORIGINAL_SYSTEM_REFERENCE,
  SERIALIZATION_START_OP,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    ROUTING_SEQUENCE_ID,
    ASSEMBLY_ITEM_ID,
    ORGANIZATION_ID,
    ALTERNATE_ROUTING_DESIGNATOR,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    ROUTING_TYPE,
    COMMON_ASSEMBLY_ITEM_ID,
    COMMON_ROUTING_SEQUENCE_ID,
    ROUTING_COMMENT,
    COMPLETION_SUBINVENTORY,
    COMPLETION_LOCATOR_ID,
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
    LINE_ID,
    CFM_ROUTING_FLAG,
    MIXED_MODEL_MAP_FLAG,
    PRIORITY,
    TOTAL_PRODUCT_CYCLE_TIME,
    CTP_FLAG,
    PROJECT_ID,
    TASK_ID,
    PENDING_FROM_ECN,
    ORIGINAL_SYSTEM_REFERENCE,
    SERIALIZATION_START_OP,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.bom_operational_routings
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ROUTING_SEQUENCE_ID, '0'), kca_seq_id) IN (
      SELECT
        COALESCE(ROUTING_SEQUENCE_ID, '0') AS ROUTING_SEQUENCE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.bom_operational_routings
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ROUTING_SEQUENCE_ID, '0')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.bom_operational_routings SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.bom_operational_routings SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ROUTING_SEQUENCE_ID
  ) IN (
    SELECT
      ROUTING_SEQUENCE_ID
    FROM bec_raw_dl_ext.bom_operational_routings
    WHERE
      (ROUTING_SEQUENCE_ID, KCA_SEQ_ID) IN (
        SELECT
          ROUTING_SEQUENCE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.bom_operational_routings
        GROUP BY
          ROUTING_SEQUENCE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'bom_operational_routings';