DROP TABLE IF EXISTS silver_bec_ods.wf_runnable_processes_v;
CREATE TABLE IF NOT EXISTS silver_bec_ods.wf_runnable_processes_v (
  ITEM_TYPE STRING,
  PROCESS_NAME STRING,
  DISPLAY_NAME STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.wf_runnable_processes_v (
  ITEM_TYPE,
  PROCESS_NAME,
  DISPLAY_NAME,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  ITEM_TYPE,
  PROCESS_NAME,
  DISPLAY_NAME,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.wf_runnable_processes_v;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wf_runnable_processes_v';