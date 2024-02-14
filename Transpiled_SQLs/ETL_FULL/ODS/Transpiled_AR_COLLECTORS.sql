DROP TABLE IF EXISTS silver_bec_ods.AR_COLLECTORS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.AR_COLLECTORS (
  collector_id DECIMAL(15, 0),
  last_updated_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_update_login DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  name STRING,
  employee_id DECIMAL(15, 0),
  description STRING,
  status STRING,
  inactive_date TIMESTAMP,
  alias STRING,
  telephone_number STRING,
  attribute_category STRING,
  attribute1 STRING,
  attribute2 STRING,
  attribute3 STRING,
  attribute4 STRING,
  attribute5 STRING,
  attribute6 STRING,
  attribute7 STRING,
  attribute8 STRING,
  attribute9 STRING,
  attribute10 STRING,
  attribute11 STRING,
  attribute12 STRING,
  attribute13 STRING,
  attribute14 STRING,
  attribute15 STRING,
  resource_id DECIMAL(15, 0),
  resource_type STRING,
  ZD_EDITION_NAME STRING,
  ZD_SYNC STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.AR_COLLECTORS (
  collector_id,
  last_updated_by,
  last_update_date,
  last_update_login,
  creation_date,
  created_by,
  `name`,
  employee_id,
  description,
  status,
  inactive_date,
  alias,
  telephone_number,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  resource_id,
  resource_type,
  zd_edition_name,
  zd_sync,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  collector_id,
  last_updated_by,
  last_update_date,
  last_update_login,
  creation_date,
  created_by,
  `name`,
  employee_id,
  description,
  status,
  inactive_date,
  alias,
  telephone_number,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  resource_id,
  resource_type,
  zd_edition_name,
  zd_sync,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.AR_COLLECTORS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ar_collectors';