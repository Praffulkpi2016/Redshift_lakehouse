DROP table IF EXISTS silver_bec_ods.ALR_OUTPUT_HISTORY;
CREATE TABLE IF NOT EXISTS silver_bec_ods.alr_output_history (
  APPLICATION_ID DECIMAL(15, 0),
  NAME STRING,
  CHECK_ID DECIMAL(15, 0),
  ROW_NUMBER DECIMAL(15, 0),
  DATA_TYPE STRING,
  VALUE STRING,
  SECURITY_GROUP_ID DECIMAL(15, 0),
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.alr_output_history (
  APPLICATION_ID,
  NAME,
  CHECK_ID,
  ROW_NUMBER,
  DATA_TYPE,
  VALUE,
  SECURITY_GROUP_ID,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    APPLICATION_ID,
    NAME,
    CHECK_ID,
    ROW_NUMBER,
    DATA_TYPE,
    VALUE,
    SECURITY_GROUP_ID,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.alr_output_history
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'alr_output_history';