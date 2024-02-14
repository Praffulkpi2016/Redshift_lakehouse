DROP TABLE IF EXISTS silver_bec_ods.POBV_BUYERS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.POBV_BUYERS (
  buyer_id DECIMAL(9, 0),
  buyer_employee_number STRING,
  buyer_name STRING,
  authorization_limit DECIMAL(15, 0),
  start_effective_date TIMESTAMP,
  end_effective_date TIMESTAMP,
  category_id DECIMAL(15, 0),
  location_id DECIMAL(15, 0),
  created_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_updated_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.POBV_BUYERS (
  buyer_id,
  buyer_employee_number,
  buyer_name,
  authorization_limit,
  start_effective_date,
  end_effective_date,
  category_id,
  location_id,
  created_date,
  created_by,
  last_updated_date,
  last_updated_by,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  buyer_id,
  buyer_employee_number,
  buyer_name,
  authorization_limit,
  start_effective_date,
  end_effective_date,
  category_id,
  location_id,
  created_date,
  created_by,
  last_updated_date,
  last_updated_by,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.POBV_BUYERS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'pobv_buyers';