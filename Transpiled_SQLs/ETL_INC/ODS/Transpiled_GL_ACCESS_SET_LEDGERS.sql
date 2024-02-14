TRUNCATE table silver_bec_ods.GL_ACCESS_SET_LEDGERS;
INSERT INTO silver_bec_ods.gl_access_set_ledgers (
  access_set_id,
  ledger_id,
  access_privilege_code,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  start_date,
  end_date,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    access_set_id,
    ledger_id,
    access_privilege_code,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    start_date,
    end_date,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.gl_access_set_ledgers
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_access_set_ledgers';