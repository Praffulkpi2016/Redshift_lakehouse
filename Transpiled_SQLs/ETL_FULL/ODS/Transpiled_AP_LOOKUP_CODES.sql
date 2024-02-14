DROP TABLE IF EXISTS silver_bec_ods.AP_LOOKUP_CODES;
CREATE TABLE IF NOT EXISTS silver_bec_ods.AP_LOOKUP_CODES (
  lookup_type STRING,
  lookup_code STRING,
  displayed_field STRING,
  description STRING,
  enabled_flag STRING,
  start_date_active TIMESTAMP,
  inactive_date TIMESTAMP,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.AP_LOOKUP_CODES (
  lookup_type,
  lookup_code,
  displayed_field,
  description,
  enabled_flag,
  start_date_active,
  inactive_date,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  lookup_type,
  lookup_code,
  displayed_field,
  description,
  enabled_flag,
  start_date_active,
  inactive_date,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.AP_LOOKUP_CODES;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_lookup_codes';