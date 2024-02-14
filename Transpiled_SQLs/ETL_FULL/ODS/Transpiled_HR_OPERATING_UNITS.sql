DROP TABLE IF EXISTS silver_bec_ods.HR_OPERATING_UNITS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.HR_OPERATING_UNITS (
  business_group_id DECIMAL(15, 0),
  organization_id DECIMAL(15, 0),
  name STRING,
  date_from TIMESTAMP,
  date_to TIMESTAMP,
  short_code STRING,
  set_of_books_id STRING,
  default_legal_context_id STRING,
  usable_flag STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.HR_OPERATING_UNITS (
  business_group_id,
  organization_id,
  `name`,
  date_from,
  date_to,
  short_code,
  set_of_books_id,
  default_legal_context_id,
  usable_flag,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  business_group_id,
  organization_id,
  `name`,
  date_from,
  date_to,
  short_code,
  set_of_books_id,
  default_legal_context_id,
  usable_flag,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.HR_OPERATING_UNITS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'hr_operating_units';