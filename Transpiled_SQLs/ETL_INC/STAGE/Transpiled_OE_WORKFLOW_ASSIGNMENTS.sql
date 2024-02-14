TRUNCATE table bronze_bec_ods_stg.oe_workflow_assignments;
INSERT INTO bronze_bec_ods_stg.oe_workflow_assignments (
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
  kca_seq_id,
  KCA_SEQ_DATE
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
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.oe_workflow_assignments
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ASSIGNMENT_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(ASSIGNMENT_ID, 0) AS ASSIGNMENT_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.oe_workflow_assignments
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ASSIGNMENT_ID, 0)
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'oe_workflow_assignments'
      )
    )
);