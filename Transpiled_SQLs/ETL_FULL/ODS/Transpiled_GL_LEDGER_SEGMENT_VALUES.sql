DROP TABLE IF EXISTS silver_bec_ods.GL_LEDGER_SEGMENT_VALUES;
CREATE TABLE IF NOT EXISTS silver_bec_ods.GL_LEDGER_SEGMENT_VALUES (
  ledger_id DECIMAL(15, 0),
  segment_type_code STRING,
  segment_value STRING,
  parent_record_id DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  end_date TIMESTAMP,
  start_date TIMESTAMP,
  status_code STRING,
  legal_entity_id DECIMAL(15, 0),
  sla_sequencing_flag STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.GL_LEDGER_SEGMENT_VALUES (
  ledger_id,
  segment_type_code,
  segment_value,
  parent_record_id,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  end_date,
  start_date,
  status_code,
  legal_entity_id,
  sla_sequencing_flag,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  ledger_id,
  segment_type_code,
  segment_value,
  parent_record_id,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  end_date,
  start_date,
  status_code,
  legal_entity_id,
  sla_sequencing_flag,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.GL_LEDGER_SEGMENT_VALUES;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_ledger_segment_values';