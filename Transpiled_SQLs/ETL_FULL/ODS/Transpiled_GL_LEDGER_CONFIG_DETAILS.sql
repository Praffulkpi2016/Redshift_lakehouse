DROP TABLE IF EXISTS silver_bec_ods.GL_LEDGER_CONFIG_DETAILS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.GL_LEDGER_CONFIG_DETAILS (
  configuration_id DECIMAL(15, 0),
  object_type_code STRING,
  object_id DECIMAL(15, 0),
  object_name STRING,
  setup_step_code STRING,
  next_action_code STRING,
  status_code STRING,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.GL_LEDGER_CONFIG_DETAILS (
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
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
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
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.GL_LEDGER_CONFIG_DETAILS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_ledger_config_details';