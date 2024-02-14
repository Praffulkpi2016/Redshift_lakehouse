TRUNCATE table silver_bec_ods.wf_runnable_processes_v;
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
  KCA_SEQ_DATE
FROM bronze_bec_ods_stg.wf_runnable_processes_v;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wf_runnable_processes_v';