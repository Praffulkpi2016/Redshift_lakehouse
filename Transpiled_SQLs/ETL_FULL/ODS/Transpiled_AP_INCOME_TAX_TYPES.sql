DROP TABLE IF EXISTS silver_bec_ods.ap_income_tax_types;
CREATE TABLE IF NOT EXISTS silver_bec_ods.ap_income_tax_types (
  income_tax_type STRING,
  description STRING,
  inactive_date TIMESTAMP,
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  ZD_EDITION_NAME STRING,
  ZD_SYNC STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.ap_income_tax_types (
  income_tax_type,
  description,
  inactive_date,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  income_tax_type,
  description,
  inactive_date,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.ap_income_tax_types;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_income_tax_types';