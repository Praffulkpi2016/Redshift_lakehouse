/* Delete Records */
DELETE FROM silver_bec_ods.oe_workflow_assignments
WHERE
  (
    COALESCE(ASSIGNMENT_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.ASSIGNMENT_ID, 0) AS ASSIGNMENT_ID
    FROM silver_bec_ods.oe_workflow_assignments AS ods, bronze_bec_ods_stg.oe_workflow_assignments AS stg
    WHERE
      COALESCE(ods.ASSIGNMENT_ID, 0) = COALESCE(stg.ASSIGNMENT_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.oe_workflow_assignments (
  ORDER_TYPE_ID,
  LINE_TYPE_ID,
  PROCESS_NAME,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  REQUEST_ID,
  CONTEXT,
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
  ITEM_TYPE_CODE,
  ASSIGNMENT_ID,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  WF_ITEM_TYPE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    ORDER_TYPE_ID,
    LINE_TYPE_ID,
    PROCESS_NAME,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    REQUEST_ID,
    CONTEXT,
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
    ITEM_TYPE_CODE,
    ASSIGNMENT_ID,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    WF_ITEM_TYPE,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.oe_workflow_assignments
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ASSIGNMENT_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(ASSIGNMENT_ID, 0) AS ASSIGNMENT_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.oe_workflow_assignments
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ASSIGNMENT_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.oe_workflow_assignments SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.oe_workflow_assignments SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ASSIGNMENT_ID
  ) IN (
    SELECT
      ASSIGNMENT_ID
    FROM bec_raw_dl_ext.oe_workflow_assignments
    WHERE
      (ASSIGNMENT_ID, KCA_SEQ_ID) IN (
        SELECT
          ASSIGNMENT_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.oe_workflow_assignments
        GROUP BY
          ASSIGNMENT_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oe_workflow_assignments';