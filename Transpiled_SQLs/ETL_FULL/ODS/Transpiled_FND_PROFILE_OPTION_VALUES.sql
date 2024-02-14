DROP TABLE IF EXISTS silver_bec_ods.FND_PROFILE_OPTION_VALUES;
CREATE TABLE IF NOT EXISTS silver_bec_ods.FND_PROFILE_OPTION_VALUES (
  application_id DECIMAL(15, 0),
  profile_option_id DECIMAL(15, 0),
  level_id DECIMAL(15, 0),
  level_value DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  profile_option_value STRING,
  level_value_application_id DECIMAL(15, 0),
  level_value2 DECIMAL(15, 0),
  zd_edition_name STRING,
  zd_sync STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.FND_PROFILE_OPTION_VALUES (
  application_id,
  profile_option_id,
  level_id,
  level_value,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  profile_option_value,
  level_value_application_id,
  level_value2,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  application_id,
  profile_option_id,
  level_id,
  level_value,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  profile_option_value,
  level_value_application_id,
  level_value2,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.FND_PROFILE_OPTION_VALUES;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fnd_profile_option_values';