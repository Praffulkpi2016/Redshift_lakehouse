TRUNCATE table bronze_bec_ods_stg.PO_ACTION_HISTORY;
INSERT INTO bronze_bec_ods_stg.PO_ACTION_HISTORY (
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
  kca_seq_id,
  KCA_SEQ_DATE
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
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.PO_ACTION_HISTORY
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (OBJECT_ID, OBJECT_TYPE_CODE, OBJECT_SUB_TYPE_CODE, SEQUENCE_NUM, kca_seq_id) IN (
      SELECT
        OBJECT_ID,
        OBJECT_TYPE_CODE,
        OBJECT_SUB_TYPE_CODE,
        SEQUENCE_NUM,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.PO_ACTION_HISTORY
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        OBJECT_ID,
        OBJECT_TYPE_CODE,
        OBJECT_SUB_TYPE_CODE,
        SEQUENCE_NUM
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'po_action_history '
      )
    )
);