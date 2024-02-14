/* Delete Records */
DELETE FROM silver_bec_ods.PO_ACTION_HISTORY
WHERE
  (OBJECT_ID, OBJECT_TYPE_CODE, OBJECT_SUB_TYPE_CODE, SEQUENCE_NUM) IN (
    SELECT
      stg.OBJECT_ID,
      stg.OBJECT_TYPE_CODE,
      stg.OBJECT_SUB_TYPE_CODE,
      stg.SEQUENCE_NUM
    FROM silver_bec_ods.PO_ACTION_HISTORY AS ods, bronze_bec_ods_stg.PO_ACTION_HISTORY AS stg
    WHERE
      ods.OBJECT_ID = stg.OBJECT_ID
      AND ods.OBJECT_TYPE_CODE = stg.OBJECT_TYPE_CODE
      AND ods.OBJECT_SUB_TYPE_CODE = stg.OBJECT_SUB_TYPE_CODE
      AND ods.SEQUENCE_NUM = stg.SEQUENCE_NUM
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.PO_ACTION_HISTORY (
  object_id,
  object_type_code,
  object_sub_type_code,
  sequence_num,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  action_code,
  action_date,
  employee_id,
  approval_path_id,
  note,
  object_revision_num,
  offline_code,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  program_date,
  approval_group_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    object_id,
    object_type_code,
    object_sub_type_code,
    sequence_num,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    action_code,
    action_date,
    employee_id,
    approval_path_id,
    note,
    object_revision_num,
    offline_code,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    program_date,
    approval_group_id,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.PO_ACTION_HISTORY
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (OBJECT_ID, OBJECT_TYPE_CODE, OBJECT_SUB_TYPE_CODE, SEQUENCE_NUM, kca_seq_id) IN (
      SELECT
        OBJECT_ID,
        OBJECT_TYPE_CODE,
        OBJECT_SUB_TYPE_CODE,
        SEQUENCE_NUM,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.PO_ACTION_HISTORY
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        OBJECT_ID,
        OBJECT_TYPE_CODE,
        OBJECT_SUB_TYPE_CODE,
        SEQUENCE_NUM
    )
);
/* Soft delete */
UPDATE silver_bec_ods.PO_ACTION_HISTORY SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.PO_ACTION_HISTORY SET IS_DELETED_FLG = 'Y'
WHERE
  (OBJECT_ID, OBJECT_TYPE_CODE, OBJECT_SUB_TYPE_CODE, SEQUENCE_NUM) IN (
    SELECT
      OBJECT_ID,
      OBJECT_TYPE_CODE,
      OBJECT_SUB_TYPE_CODE,
      SEQUENCE_NUM
    FROM bec_raw_dl_ext.PO_ACTION_HISTORY
    WHERE
      (OBJECT_ID, OBJECT_TYPE_CODE, OBJECT_SUB_TYPE_CODE, SEQUENCE_NUM, KCA_SEQ_ID) IN (
        SELECT
          OBJECT_ID,
          OBJECT_TYPE_CODE,
          OBJECT_SUB_TYPE_CODE,
          SEQUENCE_NUM,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.PO_ACTION_HISTORY
        GROUP BY
          OBJECT_ID,
          OBJECT_TYPE_CODE,
          OBJECT_SUB_TYPE_CODE,
          SEQUENCE_NUM
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_action_history ';