DROP TABLE IF EXISTS silver_bec_ods.HR_LOCATIONS_ALL_TL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.HR_LOCATIONS_ALL_TL (
  location_id DECIMAL(15, 0),
  `language` STRING,
  source_lang STRING,
  location_code STRING,
  description STRING,
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  created_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.HR_LOCATIONS_ALL_TL (
  location_id,
  `language`,
  source_lang,
  location_code,
  description,
  last_update_date,
  last_updated_by,
  last_update_login,
  created_by,
  creation_date,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  location_id,
  `language`,
  source_lang,
  location_code,
  description,
  last_update_date,
  last_updated_by,
  last_update_login,
  created_by,
  creation_date,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.HR_LOCATIONS_ALL_TL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'hr_locations_all_tl';