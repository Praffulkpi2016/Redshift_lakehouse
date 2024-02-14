TRUNCATE table bronze_bec_ods_stg.GL_LEDGER_CONFIG_DETAILS;
INSERT INTO bronze_bec_ods_stg.GL_LEDGER_CONFIG_DETAILS (
  configuration_id,
  object_type_code,
  object_id,
  object_name,
  setup_step_code,
  next_action_code,
  status_code,
  created_by,
  last_update_login,
  last_update_date,
  last_updated_by,
  creation_date,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    configuration_id,
    object_type_code,
    object_id,
    object_name,
    setup_step_code,
    next_action_code,
    status_code,
    created_by,
    last_update_login,
    last_update_date,
    last_updated_by,
    creation_date,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.GL_LEDGER_CONFIG_DETAILS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE, kca_seq_id) IN (
      SELECT
        CONFIGURATION_ID,
        OBJECT_ID,
        OBJECT_TYPE_CODE,
        SETUP_STEP_CODE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.GL_LEDGER_CONFIG_DETAILS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        CONFIGURATION_ID,
        OBJECT_ID,
        OBJECT_TYPE_CODE,
        SETUP_STEP_CODE
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_ledger_config_details'
      )
    )
);